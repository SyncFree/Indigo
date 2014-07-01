package swift.indigo.remote;

import static sys.Context.Networking;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.api.TxnHandle;
import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.ReturnableTimestampSourceDecorator;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.TxnStatus;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.indigo.AbstractTxHandle;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.Lock;
import swift.indigo.proto.AcquireLocksReply;
import swift.indigo.proto.AcquireLocksRequest;
import swift.indigo.proto.DiscardSnapshotRequest;
import swift.indigo.proto.IndigoCommitRequest;
import swift.indigo.proto.ReleaseLocksRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.FetchObjectVersionReply;
import swift.proto.FetchObjectVersionRequest;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.utils.Threading;

public class RemoteIndigo implements Indigo {
	private static Logger Log = Logger.getLogger(RemoteIndigo.class.getName());

	final Endpoint server;
	final Service stub;

	final String stubId;
	final ReturnableTimestampSourceDecorator<Timestamp> tsSource;

	_TxnHandle handle;

	public static Indigo getInstance(Endpoint server) {
		return new RemoteIndigo(server);
	}

	RemoteIndigo(Endpoint server) {
		this.server = server;
		this.stub = Networking.stub();
		this.stubId = this.stub.localEndpoint().url();
		this.tsSource = new ReturnableTimestampSourceDecorator<Timestamp>(new IncrementalTimestampGenerator(stubId));
	}

	public TxnHandle getTxnHandle() {
		return handle;
	}

	@Override
	public void beginTxn(Lock... locks) {
		beginTxn(locks, null);
	}

	@Override
	public void beginTxn(Lock[] locks, CounterReservation[] counters) {
		Timestamp txnTimestamp = tsSource.generateNew();

		AcquireLocksRequest request = new AcquireLocksRequest(stubId, txnTimestamp, locks, counters);
		for (int delay = 50;; delay = Math.min(1000, 2 * delay)) {
			AcquireLocksReply reply = stub.request(server, request);
			if (reply != null && reply.acquiredLocksAndCounters()) {
				handle = new _TxnHandle(reply, request.cltTimestamp(), locks != null && locks.length > 0);
				break;
			} else
				Threading.sleep(delay);
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

	public <V extends CRDT<V>> V get(CRDTIdentifier id) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
		return get(id, false, null);
	}

	public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
		return (V) handle.get(id, create, classOfV);
	}

	class _TxnHandle extends AbstractTxHandle {
		final long serial;

		boolean withLocks;
		Timestamp timestamp;

		_TxnHandle(AcquireLocksReply reply, Timestamp cltTimestamp, boolean withLocks) {
			super(reply.getSnapshot(), cltTimestamp);
			this.withLocks = withLocks;
			this.timestamp = reply.timestamp();

			for (CRDTObjectUpdatesGroup<?> i : reply.lockOps())
				super.ops.put(i.getTargetUID(), i);

			this.serial = reply.serial();
		}

		public void rollback() {
			if (withLocks)
				stub.send(server, new ReleaseLocksRequest(serial, stubId, cltTimestamp));
			else
				stub.send(server, new DiscardSnapshotRequest(serial, stubId, cltTimestamp));

			tsSource.returnLastTimestamp();
		}

		@Override
		public void commit() {

			List<CRDTObjectUpdatesGroup<?>> updates = getUpdates();

			if (!updates.isEmpty()) {
				final IndigoCommitRequest req = new IndigoCommitRequest(serial, stubId, cltTimestamp, snapshot, updates);
				req.setTimestamps(timestamp, null);

				final Semaphore semaphore = new Semaphore(0);
				stub.asyncRequest(server, req, (CommitUpdatesReply reply) -> {
					if (reply.getStatus() == CommitUpdatesReply.CommitStatus.INVALID_OPERATION)
						Log.warning("FAILED COMMIT-------------->>>>>>>>>" + reply.getStatus() + " FOR : " + reply.getCommitTimestamps().get(0) + " FOR " + req.getObjectUpdateGroups());

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
		protected <V extends CRDT<V>> ManagedCRDT<V> getCRDT(CRDTIdentifier uid, CausalityClock version, boolean create, Class<V> classOfV) {

			FetchObjectVersionRequest req = new FetchObjectVersionRequest(stubId, uid, version, false);

			FetchObjectVersionReply reply = stub.request(server, req);
			if (reply != null) {
				if (reply.getStatus() == FetchObjectVersionReply.FetchStatus.OK) {
					return (ManagedCRDT<V>) reply.getCrdt();
				}
				if (reply.getStatus() == FetchObjectVersionReply.FetchStatus.OBJECT_NOT_FOUND) {
					return createCRDT(uid, snapshot, classOfV);
				}
				System.err.println(version + "   ------------>>>>>" + reply.getStatus());
			}
			throw new RuntimeException("Unexpected Error while fetching: " + uid);
		}
	}

}