package swift.indigo;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.dc.Sequencer;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.indigo.proto.FetchObjectReply;
import swift.indigo.proto.FetchObjectRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesRequest;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.utils.Timings;

public class StorageHelper {
	private static Logger Log = Logger.getLogger(StorageHelper.class.getName());

	final String LOCK_MANAGER;
	final Service stub;
	final Sequencer sequencer;

	private final CausalityClock grantedRequests;

	private String resourceMgrId;
	private IncrementalTimestampGenerator cltTimestampSource;

	private Map<Class, Class> tableToType;

	private boolean isMasterLockManager;

	final private Endpoint surrogate;

	// TODO: Sequencer should be remote
	public StorageHelper(final Sequencer sequencer, Endpoint surrogate, String resourceMgrId,
			boolean isMasterLockManager) {
		this.sequencer = sequencer;
		this.LOCK_MANAGER = sequencer.siteId + "LockManager";
		this.stub = sequencer.stub;
		this.isMasterLockManager = isMasterLockManager;
		this.surrogate = surrogate;

		this.grantedRequests = sequencer.clocks.currentClockCopy();
		this.cltTimestampSource = new IncrementalTimestampGenerator(resourceMgrId, 0L);
		this.tableToType = new HashMap<Class, Class>();
	}

	public <A extends ResourceRequest<?>, B extends CRDT<?>> void registerType(Class<A> requestType, Class<B> theClass) {
		this.tableToType.put(requestType, theClass);
	}

	synchronized CausalityClock getCurrentClock() {
		return sequencer.clocks.currentClockCopy();
	}

	_TxnHandle beginTxn(Timestamp cltTimestamp) {
		return new _TxnHandle(getCurrentClock(), cltTimestamp);
	}

	public void endTxn(final _TxnHandle handle, final boolean writeThrough) {
		if (writeThrough)
			handle.commit();
	}

	public Resource getResource(ResourceRequest<?> req, final _TxnHandle handle) throws SwiftException {
		Class<CRDT> type = tableToType.get(req.getClass());
		Resource resource = (Resource<?>) handle.getMostRecent(req.getResourceId(), isMasterLockManager, type);
		return resource;
	}

	// ATTENTION: Original code used locks to protect the creation of the lock
	// public void createResources(Collection<ResourceRequest<?>> request)
	// throws SwiftException {
	// for (ResourceRequest<?> req : request) {
	// createResource(req);
	// }
	// }

	// public Resource<?> createResource(ResourceRequest<?> req) throws
	// SwiftException {
	// if (Log.isLoggable(Level.INFO))
	// Log.info("Created resource for request " + req);
	// Class<CRDT> type = tableToType.get(req.getResourceId().getTable());
	// Resource resource = (Resource) handle.getMostRecent(req.getResourceId(),
	// true, type);
	// resource.initialize(sequencer.siteId, req);
	// return resource;
	// }

	class _TxnHandle extends AbstractTxHandle {

		_TxnHandle(CausalityClock snapshot, Timestamp cltTimestamp) {
			super(snapshot, cltTimestamp);
		}

		@Override
		public void commit() {
			try {

				System.out.println("COMMIT " + this.toString());
				Timings.mark();
				final CommitUpdatesRequest req;
				List<CRDTObjectUpdatesGroup<?>> updates = getUpdates();

				if (!updates.isEmpty()) {
					System.out.println("UPDATES NOT EMPTY " + updates.size());
					Timestamp ts = sequencer.clocks.newTimestamp();

					req = new CommitUpdatesRequest(LOCK_MANAGER + "-" + sequencer.siteId, cltTimestamp(), snapshot,
							updates);
					req.setTimestamp(ts);
				} else {
					System.out.println("UPDATES EMPTY");
					req = new CommitUpdatesRequest(LOCK_MANAGER + "-" + sequencer.siteId, new Timestamp("dummy", -1L),
							snapshot, updates);
					Thread.currentThread().dumpStack();
					System.out.println("NAO pode acontecer");
					System.exit(0);
				}

				final Semaphore semaphore = new Semaphore(0);
				stub.asyncRequest(surrogate, req, (CommitUpdatesReply r) -> {
					if (Log.isLoggable(Level.INFO)) {
						Timestamp test = req.getTimestamp();
						if (test == null) {
							System.out.println("aqui");
						}
						Log.info("FINISH COMMIT------->>" + r.getStatus() + " FOR : " + req.getTimestamp());
					}
					semaphore.release();
				});
				semaphore.acquireUninterruptibly();
			} finally {
				Timings.sample("async commit");
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		protected <V extends CRDT<V>> ManagedCRDT<V> getCRDT(CRDTIdentifier uid, CausalityClock version,
				boolean create, Class<V> classOfV) throws VersionNotFoundException {
			try {
				Timings.mark();
				FetchObjectRequest req = new FetchObjectRequest(getCurrentClock(), LOCK_MANAGER, uid, true);

				Timings.mark();
				FetchObjectReply reply = stub.request(surrogate, req);
				Timings.sample("fetchCRDT(" + uid + ") from" + surrogate);

				if (reply != null) {
					if (reply.getStatus() == FetchObjectReply.FetchStatus.OK) {
						ManagedCRDT<V> res = (ManagedCRDT<V>) reply.getCrdt();
						CMP_CLOCK cmp = version.compareTo(res.getClock());
						if (cmp.is(CMP_CLOCK.CMP_EQUALS, CMP_CLOCK.CMP_ISDOMINATED))
							return res;
						else
							throw new VersionNotFoundException("Version nout found");
					}
					if (create && reply.getStatus() == FetchObjectReply.FetchStatus.OBJECT_NOT_FOUND) {
						return createCRDT(uid, version, classOfV);
					}
				}
				return null;
			} finally {
				Timings.sample("getCRDT(" + uid + ")");
			}
		}
		public <V extends CRDT<V>> V getMostRecent(CRDTIdentifier id, boolean create, Class<V> classOfV)
				throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {

			return (V) this.getCRDT(id, getCurrentClock(), create, classOfV).getLatestVersion(this);
		}

		protected Timestamp cltTimestamp() {
			if (cltTimestamp == null)
				cltTimestamp = cltTimestampSource.generateNew();

			return cltTimestamp;
		}
	}

	public Timestamp recordNewEvent() {
		Timestamp ts = sequencer.clocks.newTimestamp(); // short circuit
		// sequencer, is it
		// safe????
		if (ts != null)
			synchronized (grantedRequests) {
				grantedRequests.record(ts);
			}
		return ts;
	}

	public Collection<CRDTObjectUpdatesGroup<?>> endTxnAndGetUpdates(final _TxnHandle handle, boolean b) {
		endTxn(handle, b);
		return handle.getUpdates();
	}

}
