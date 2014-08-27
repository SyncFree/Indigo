package swift.indigo.remote;

import static sys.Context.Networking;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.api.TxnHandle;
import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.ReturnableTimestampSourceDecorator;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTOperationDependencyPolicy;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.TxnStatus;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.indigo.AbstractTxHandle;
import swift.indigo.Indigo;
import swift.indigo.ResourceRequest;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.DiscardSnapshotRequest;
import swift.indigo.proto.IndigoCommitRequest;
import swift.indigo.proto.ReleaseResourcesRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.FetchObjectVersionReply;
import swift.proto.FetchObjectVersionRequest;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.utils.Args;
import sys.utils.Profiler;
import sys.utils.Threading;

public class RemoteIndigo implements Indigo {
	private static Logger Log = Logger.getLogger(RemoteIndigo.class.getName());
	private static Profiler profiler;
	private static final String resultsLogName = "RemoteIndigoResults";

	final Endpoint server;
	final Service stub;

	final String stubId;
	final ReturnableTimestampSourceDecorator<Timestamp> tsSource;

	_TxnHandle handle;

	// private Timestamp lastTSWithGrantedLocks;

	public static Indigo getInstance(Endpoint server) {
		return new RemoteIndigo(server);
	}

	public static Indigo getInstance(String server) {
		return new RemoteIndigo(Networking.resolve(server));
	}

	RemoteIndigo(Endpoint server) {
		this.server = server;
		this.stub = Networking.stub();
		this.stubId = this.stub.localEndpoint().url();
		this.tsSource = new ReturnableTimestampSourceDecorator<Timestamp>(new IncrementalTimestampGenerator(stubId));
		if (profiler == null) {
			initializeProfiling(resultsLogName);
		}
	}

	private void initializeProfiling(String resultsLogName) {
		profiler = Profiler.getInstance();
		Logger logger = Logger.getLogger(resultsLogName);
		if (logger.isLoggable(Level.FINEST)) {
			FileHandler fileTxt;
			try {
				String resultsDir = Args.valueOf("-results_dir", ".");
				String siteId = Args.valueOf("-site_id", "GLOBAL");
				String suffix = Args.valueOf("-fileNameSuffix", "");
				fileTxt = new FileHandler(resultsDir + "/remote_indigo_results" + "_" + siteId + suffix + ".log");
				fileTxt.setFormatter(new java.util.logging.Formatter() {

					@Override
					public String format(LogRecord record) {
						return record.getMessage() + "\n";
					}
				});
				logger.addHandler(fileTxt);
			} catch (Exception e) {
				Log.warning("Exception while generating log file");
				System.exit(0);
			}
		}
		profiler.printHeaderWithCustomFields(resultsLogName, "RESULT", "RETRIES", "TIMESTAMP");
	}

	public TxnHandle getTxnHandle() {
		return handle;
	}

	public void beginTxn() throws SwiftException {
		beginTxn(new HashSet<ResourceRequest<?>>());
	}

	@Override
	public void beginTxn(Collection<ResourceRequest<?>> resources) throws IndigoImpossibleExcpetion {
		long opId = profiler.startOp(resultsLogName, "beginTxn");
		Timestamp txnTimestamp = tsSource.generateNew();
		// while (txnTimestamp.equals(lastTSWithGrantedLocks)) {
		// Log.warning("Repeated Timestamp " + lastTSWithGrantedLocks);
		// txnTimestamp = tsSource.generateNewForced();
		// }

		AcquireResourcesRequest request = new AcquireResourcesRequest(stubId, txnTimestamp, resources);

		for (ResourceRequest<?> res : resources) {
			res.setClientTs(txnTimestamp);
		}

		int retryCount = 0;
		for (int delay = /* 20 */250;; delay = Math.min(1000, 2 * delay)) {
			AcquireResourcesReply reply = stub.request(server, request);
			if (reply != null && reply.acquiredResources() || resources.size() == 0) {
				handle = new _TxnHandle(reply, request.getClientTs(), resources != null && resources.size() > 0);
				profiler.endOp(opId, reply.acquiredStatus().toString(), "" + retryCount, txnTimestamp.toString());
				// if (resources.size() != 0)
				// lastTSWithGrantedLocks = txnTimestamp;
				break;
			} else if (reply.isImpossible()) {
				throw new IndigoImpossibleExcpetion();
			} else {
				retryCount++;
				Threading.sleep(delay);
			}
		}
	}
	public void endTxn() {
		if (handle != null)
			handle.commit();
		handle = null;
	}

	public void abortTxn() {
		if (handle != null)
			handle.rollback();
		handle = null;
	}

	public <V extends CRDT<V>> V get(CRDTIdentifier id) throws WrongTypeException, NoSuchObjectException,
			VersionNotFoundException, NetworkException {
		return get(id, false, null);
	}

	public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV) throws WrongTypeException,
			NoSuchObjectException, VersionNotFoundException, NetworkException {
		return (V) handle.get(id, create, classOfV);
	}

	class _TxnHandle extends AbstractTxHandle {
		final long serial;

		boolean withLocks;
		Timestamp timestamp;

		_TxnHandle(AcquireResourcesReply reply, Timestamp cltTimestamp, boolean withLocks) {
			super(reply.getSnapshot(), cltTimestamp);
			this.withLocks = withLocks;
			this.timestamp = reply.timestamp();

			for (CRDTObjectUpdatesGroup<?> i : reply.operations()) {
				// Puts new objects in cache so that they can be used
				// immediately.
				if (!cache.containsKey(i.getTargetUID()) && i.getCreationState() != null) {
					ManagedCRDT crdt = new ManagedCRDT(i.getTargetUID(), i.getCreationState(), snapshot, false);
					crdt.execute(i, CRDTOperationDependencyPolicy.RECORD_BLINDLY);
					cache.put(i.getTargetUID(), crdt.getLatestVersion(this));
				}
				super.ops.put(i.getTargetUID(), i);
			}

			this.serial = reply.serial();
		}

		public void rollback() {
			if (withLocks)
				stub.send(server, new ReleaseResourcesRequest(serial, stubId, cltTimestamp));
			else
				stub.send(server, new DiscardSnapshotRequest(serial, stubId, cltTimestamp));

			tsSource.returnLastTimestamp();
		}

		@Override
		public void commit() {

			List<CRDTObjectUpdatesGroup<?>> updates = getUpdates();

			if (!updates.isEmpty()) {
				final IndigoCommitRequest req = new IndigoCommitRequest(serial, stubId, cltTimestamp, snapshot,
						updates, withLocks);
				req.setTimestamp(timestamp);

				final Semaphore semaphore = new Semaphore(0);
				stub.asyncRequest(
						server,
						req,
						(CommitUpdatesReply reply) -> {
							// System.out.println("Received Reply for: " +
							// cltTimestamp);
							if (reply.getStatus() == CommitUpdatesReply.CommitStatus.INVALID_OPERATION)
								Log.warning("FAILED COMMIT-------------->>>>>>>>>" + reply.getStatus() + " FOR : "
										+ reply.getCommitTimestamps().get(0) + " FOR " + req.getObjectUpdateGroups());

							semaphore.release();
						});
				semaphore.acquireUninterruptibly();
				super.status = TxnStatus.COMMITTED_GLOBAL;
			} else {
				super.status = TxnStatus.COMMITTED_LOCAL;
				rollback();
			}
		}

		@Override
		@SuppressWarnings({"unchecked"})
		protected <V extends CRDT<V>> ManagedCRDT<V> getCRDT(CRDTIdentifier uid, CausalityClock version,
				boolean create, Class<V> classOfV) {

			FetchObjectVersionRequest req = new FetchObjectVersionRequest(stubId, uid, version, false);

			FetchObjectVersionReply reply = stub.request(server, req);
			if (reply != null) {
				if (reply.getStatus() == FetchObjectVersionReply.FetchStatus.OK) {
					return (ManagedCRDT<V>) reply.getCrdt();
				}
				if (reply.getStatus() == FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND) {
					return createCRDT(uid, snapshot, classOfV);
				}
			}
			throw new RuntimeException("Unexpected Error while fetching: " + uid);
		}
	}

}
