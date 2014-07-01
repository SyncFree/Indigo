package swift.indigo;

import static sys.Context.Networking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.LowerBoundCounterCRDT;
import swift.crdt.SharedLockCRDT;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.dc.Defaults;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.indigo.proto.AcquireLocksReply;
import swift.indigo.proto.AcquireLocksRequest;
import swift.indigo.proto.CreateLocksRequest;
import swift.indigo.proto.FetchObjectReply;
import swift.indigo.proto.FetchObjectRequest;
import swift.indigo.proto.IndigoProtocol;
import swift.indigo.proto.TransferReservationRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesRequest;
import swift.utils.Pair;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.net.impl.Url;
import sys.utils.Args;
import sys.utils.Tasks;
import sys.utils.Timings;

class IndigoLockManager implements IndigoProtocol {
	private static Logger Log = Logger.getLogger(IndigoLockManager.class.getName());

	final String LOCK_MANAGER;
	final static String LOCKS_TABLE = "/indigo/locks";

	final Service stub;
	final IndigoSequencer sequencer;

	final Map<String, Endpoint> endpoints;
	final Map<Lock, SortedSet<AcquireLocksRequest>> pending;
	final boolean isMasterLockManager;
	final Endpoint masterLockManager;

	Map<String, SharedLockCRDT> locks = new HashMap<String, SharedLockCRDT>();
	Map<Timestamp, AcquireLocksReply> replies = new HashMap<Timestamp, AcquireLocksReply>();

	Map<CRDTIdentifier, LowerBoundCounterCRDT> counters = new HashMap<CRDTIdentifier, LowerBoundCounterCRDT>();
	Map<CRDTIdentifier, Map<Timestamp, Integer>> pendingWrites;

	final IncrementalTimestampGenerator cltTimestampSource;

	final Endpoint surrogate;

	public IndigoLockManager(IndigoSequencer sequencer) {
		this.sequencer = sequencer;
		this.LOCK_MANAGER = sequencer.siteId + "LockManager";

		this.stub = sequencer.stub;

		this.pending = new ConcurrentHashMap<Lock, SortedSet<AcquireLocksRequest>>();
		this.cltTimestampSource = new IncrementalTimestampGenerator(sequencer.siteId + "-lockManager", 0L);
		this.pendingWrites = new ConcurrentHashMap<CRDTIdentifier, Map<Timestamp, Integer>>();

		this.surrogate = Networking.resolve(Defaults.SERVER_URL);
		// String master = new TreeSet<String>(endpoints.keySet()).first();
		this.isMasterLockManager = false;
		this.masterLockManager = null;// endpoints.get(master);

		this.endpoints = new HashMap<String, Endpoint>();

		Args.subList("-sequencers").forEach(it -> {
			Url u = new Url(it);
			endpoints.put(u.siteId(), Networking.resolve(it, Defaults.SEQUENCER_URL));
		});
		System.err.println(endpoints);
		Log.info("ENDPOINTS: " + endpoints);

		// stub.getFactory().toService(RpcServices.PUBSUB.ordinal(), new
		// SwiftProtocolHandler() {
		// @Override
		// public void onReceive(RpcHandle conn, PubSubNotification event) {
		// Notifyable<CRDTIdentifier> payload = event.payload();
		// if (payload instanceof UpdateNotification) {
		// UpdateNotification update = (UpdateNotification) payload;
		// if (update.info.getId().getTable().equals(LOCKS_TABLE)) {
		// // TODO: Make single interface
		// // Does not refresh the value after invalidation
		// SharedLockCRDT lock = getLock(update.info.getId().getKey(), true,
		// false);
		// if (lock != null) {
		// lock.setOutdated();
		// if (Log.isLoggable(Level.INFO))
		// Log.info("INVALIDATED: " + update.info.getId());
		// }
		// } else {
		// LowerBoundCounterCRDT counter = getCounter(update.info.getId(),
		// false, false);
		// if (counter != null) {
		// counter.setOutdated();
		// if (Log.isLoggable(Level.INFO))
		// Log.info("INVALIDATED: " + update.info.getId());
		// }
		// }
		// }
		// }
		// });
	}
	CausalityClock getCurrentClock() {
		return sequencer.clocks.currentClockCopy();
	}

	synchronized boolean releaseLocks(Timestamp cltTimestamp) {
		boolean ok = true;
		AcquireLocksReply alr = replies.remove(cltTimestamp);
		if (alr != null) {
			for (Lock i : alr.locks()) {
				SharedLockCRDT lock = getLock(i.id(), true, false);
				ok &= lock != null && lock.unlock(cltTimestamp);
			}
			for (Lock i : alr.locks()) {
				checkPendingRequest(i);
			}
			for (CounterReservation c : alr.counters()) {

				Map<Timestamp, Integer> writes = pendingWrites.get(c.getId());
				if (writes != null) {
					writes.remove(cltTimestamp);
				}
				// NEVER INVALIDATE
				// counters.get(c.getId()).setOutdated();
				if (Log.isLoggable(Level.INFO))
					Log.info("Released reservation: " + c);
			}
		}
		return ok || true; // TODO: handle cases for timestamps that do not
							// involve
							// locks...
	}

	synchronized AcquireLocksReply acquireLocks(final AcquireLocksRequest request) {
		Log.info("PROCESSING AcquireLocksRequest " + request);

		AcquireLocksReply reply = replies.get(request.cltTimestamp());

		if (Log.isLoggable(Level.INFO))
			Log.info("REPLY CACHE: " + reply);

		if (reply != null && reply.matches(request.cltTimestamp()))
			return reply;

		CausalityClock snapshot = getCurrentClock();

		final List<Lock> failed = new ArrayList<Lock>();
		final List<Lock> unknownLocks = new ArrayList<Lock>();

		final Set<CounterReservation> zeroCounters = new HashSet<CounterReservation>();
		final List<CounterReservation> failedCounters = new ArrayList<CounterReservation>();
		final List<CounterReservation> unknownCounters = new ArrayList<CounterReservation>();

		beginTxn(request.cltTimestamp());

		if (request.locks() != null) {
			for (Lock lock : request.locks()) {
				SharedLockCRDT l = getLock(lock.id(), true, false);
				if (Log.isLoggable(Level.INFO))
					Log.info("READ LOCK:" + l);
				if (l == null) {
					unknownLocks.add(lock);
					continue;
				}
				if (!lock(request, lock)) {
					failed.add(lock);
				}
			}
		}

		// Checks if the counter has enough permissions locally
		if (request.counterReservations() != null) {
			for (CounterReservation reservationRequest : request.counterReservations()) {
				LowerBoundCounterCRDT counter = getCounter(reservationRequest.getId(), true, false);
				if (counter == null) {
					unknownCounters.add(reservationRequest);
				}
				if (counter.getValue() <= 0) {
					zeroCounters.add(reservationRequest);
				} else if (!hasPermissions(reservationRequest)) {
					failedCounters.add(reservationRequest);
				}
			}
		}
		endTxn(false);

		if (failed.isEmpty() && unknownLocks.isEmpty() && unknownCounters.isEmpty() && failedCounters.isEmpty()) {
			List<CRDTObjectUpdatesGroup<?>> updates = handle.getUpdates();

			Timestamp ts = sequencer.clocks.newTimestamp();

			reply = new AcquireLocksReply(handle.cltTimestamp, ts, snapshot, handle.getUpdates(), request.locks(), request.counterReservations());
			replies.put(request.cltTimestamp(), reply);

			// Pins the reservations that are in use
			for (CounterReservation counter : request.counterReservations()) {
				if (!zeroCounters.contains(counter)) {
					Map<Timestamp, Integer> countePendingWrites = pendingWrites.get(counter.getId());
					countePendingWrites.put(request.cltTimestamp(), counter.getAmount());
					if (Log.isLoggable(Level.INFO))
						Log.info("Reserved " + counter.getAmount() + " units for counter " + counter.getId());
				} else {
					if (Log.isLoggable(Level.INFO))
						Log.info("Counter with value zero or created");
				}
			}

			return reply;
		}

		// At least one failed, so release any that might have succeeded
		// modified: If it succeeded it should not have to read from the
		// database again
		for (Lock lock : request.locks()) {
			SharedLockCRDT l = getLock(lock.id(), false, false);
			if (l != null)
				l.unlock(request.cltTimestamp());
		}

		Tasks.exec(0.0, () -> {
			if (Log.isLoggable(Level.INFO))
				Log.info("FAILED TO ACQUIRE:" + failed);

			if (!unknownLocks.isEmpty() || !unknownCounters.isEmpty()) {
				stub.send(masterLockManager, new CreateLocksRequest(sequencer.siteId, unknownLocks, unknownCounters));
			}

			Map<String, Set<Lock>> owners = new HashMap<String, Set<Lock>>();
			for (Lock lock : failed) {
				SharedLockCRDT l = getLock(lock.id(), false, false);
				// Only fetches the lock if it no longer has ownership
				if (!l.isOwner(sequencer.siteId)) {
					l = getLock(lock.id(), true, true);
				}

				if (Log.isLoggable(Level.INFO))
					Log.info("WANT LOCK: " + lock.id + "--->" + l);
				if (l != null) {
					for (String i : l.owners()) {
						Set<Lock> lockSet = owners.get(i);
						if (lockSet == null)
							owners.put(i, lockSet = new HashSet<Lock>());
						lockSet.add(lock);

						if (lock.type.isShareable())
							break;
					}
				}
			}
			if (Log.isLoggable(Level.INFO))
				Log.info("ASKING:" + owners.entrySet());
			for (Map.Entry<String, Set<Lock>> e : owners.entrySet()) {
				AcquireLocksRequest req = new AcquireLocksRequest(sequencer.siteId, e.getValue());
				String ownerId = e.getKey();
				if (!ownerId.equals(sequencer.siteId))
					stub.send(endpoints.get(ownerId), req);
			}
		});

		Tasks.exec(0.0, () -> {
			if (Log.isLoggable(Level.INFO))
				Log.info("FAILED TO RESERVE COUNTERS: " + failedCounters);

			for (CounterReservation c : failedCounters) {
				TransferReservationRequest req = new TransferReservationRequest(sequencer.siteId, c.getId());
				// Updates the prreference list
				LowerBoundCounterCRDT counter = getCounter(c.getId(), true, true);
				Queue<Pair<String, Integer>> preferred = counter.preferenceList();

				if (Log.isLoggable(Level.INFO))
					Log.info("preferred list " + preferred + " ENDPOINTS " + endpoints);

				Endpoint endpoint = endpoints.get(preferred.peek().getFirst());
				if (endpoint != null) {
					stub.send(endpoint, req);
				} else {
					if (Log.isLoggable(Level.INFO))
						Log.warning("No endpoint for manager @ " + preferred.peek().getFirst());
				}

			}
		});

		return new AcquireLocksReply(false);
	}

	private boolean lock(AcquireLocksRequest request, Lock lock) {
		SharedLockCRDT l = getLock(lock.id(), false, false);
		if (!l.lock(sequencer.siteId, lock.type(), request.cltTimestamp())) {
			l = getLock(lock.id(), true, true);
			return l.lock(sequencer.siteId, lock.type(), request.cltTimestamp());
		} else {
			return true;
		}
	}

	boolean hasPermissions(CounterReservation request) {
		LowerBoundCounterCRDT c = getCounter(request.getId(), false, false);
		if (c.availableSiteId(sequencer.siteId) - amountSpentLocally(request.getId()) - request.getAmount() >= 0) {
			return true;
		} else {
			c = getCounter(request.getId(), true, true);
			return c.availableSiteId(sequencer.siteId) - amountSpentLocally(request.getId()) - request.getAmount() >= 0;
		}
	}

	void checkPendingRequest(Lock lock) {
		SortedSet<AcquireLocksRequest> pendingReqs = pending.remove(lock);
		if (pendingReqs != null)
			for (AcquireLocksRequest i : pendingReqs) {
				updateOwnershipForRemoteDC(i);
				if (lock.type.isExclusive())
					break;
			}
	}

	synchronized void updateOwnershipForRemoteDC(AcquireLocksRequest request) {
		if (Log.isLoggable(Level.INFO))
			Log.info("PROCESSING updateOwnershipForRemoteDC " + request);

		boolean updated = false;
		beginTxn(null);
		for (Lock lock : request.locks()) {
			SharedLockCRDT l = getLock(lock.id(), false, false);
			// Log.info("READ LOCK:" + l + "   Owners :" + l.owners());
			if (l == null || l.alreadyUpdated(request.requesterId(), lock.type()))
				continue;

			if (updateLockOwnership(request, lock)) {
				pending.remove(lock);
				updated = true;
			} else {
				SortedSet<AcquireLocksRequest> pendingReqs = pending.get(lock);
				if (pendingReqs == null)
					pending.put(lock, pendingReqs = new TreeSet<AcquireLocksRequest>());
				pendingReqs.add(new AcquireLocksRequest(request.requesterId(), lock));
			}
		}
		endTxn(updated);
		// Log.info("PROCESS EXIT: " + updated + "---->>>>>>" +
		// handle.getUpdates().size());
	}

	private boolean updateLockOwnership(AcquireLocksRequest request, Lock lock) {
		SharedLockCRDT l = getLock(lock.id(), true, false);
		if (!l.updateOwnership(sequencer.siteId, request.requesterId(), lock.type())) {
			l = getLock(lock.id(), true, true);
			return l.updateOwnership(sequencer.siteId, request.requesterId(), lock.type());
		} else {
			return true;
		}
	}

	void transferReservation(final TransferReservationRequest request) {
		synchronized (this) {
			beginTxn(null);
			System.out.println("START TRANSFER");
			final LowerBoundCounterCRDT counter = getCounter(request.getCounterId(), false, false);
			System.out.println(counter);
			// TODO: abstract provisioning rules.
			int localAvailable = counter.availableSiteId(sequencer.siteId) - amountSpentLocally(request.getCounterId());
			if (localAvailable / 3 > 0) {
				if (Log.isLoggable(Level.INFO))
					Log.info("Grant " + (localAvailable / 3) + " permissions to " + request.getTargetSite());

				boolean result = counter.transfer(localAvailable / 3, sequencer.siteId, request.getTargetSite());
				System.out.println("Grant " + result + " " + (localAvailable / 3) + " permissions to " + request.getTargetSite() + " LA " + counter.availableSiteId(sequencer.siteId) + " " + counter + " "
						+ amountSpentLocally(request.getCounterId()));

				// NEVER INVALIDATE
				// counter.setOutdated();
			}
		}
		System.out.println("END TRANSFER");
		endTxn(true);

	}

	LowerBoundCounterCRDT getCounter(CRDTIdentifier id, boolean fetchFromDB, boolean checkOutdated) {
		LowerBoundCounterCRDT counter = counters.get(id);
		if ((counter == null || (checkOutdated && counter.isOutdated())) && fetchFromDB) {
			try {
				counter = handle.getMostRecent(id, isMasterLockManager, LowerBoundCounterCRDT.class);
				counters.put(id, counter);
			} catch (Exception x) {
				x.printStackTrace();
			}
		}
		counter.setTxnHandle(handle);
		return counter;
	}

	SharedLockCRDT getLock(String id, boolean fetchFromDB, boolean checkOutdated) {
		SharedLockCRDT lock = locks.get(id);
		if ((lock == null || (lock.isOutdated() && checkOutdated)) && fetchFromDB) {
			try {
				lock = (SharedLockCRDT) handle.getMostRecent(new CRDTIdentifier(LOCKS_TABLE, id), isMasterLockManager, SharedLockCRDT.class);

				if (lock != null)
					locks.put(id, lock);

			} catch (Exception x) {
				x.printStackTrace();
			}
		}
		if (lock != null)
			lock.setTxnHandle(handle);

		return lock;
	}

	private int amountSpentLocally(CRDTIdentifier lockId) {
		Map<Timestamp, Integer> pendingWritesLock = pendingWrites.get(lockId);
		if (pendingWritesLock == null) {
			pendingWritesLock = new HashMap<Timestamp, Integer>();
			pendingWrites.put(lockId, pendingWritesLock);
		}
		int amountSpentLocally = 0;
		for (Entry<Timestamp, Integer> entry : pendingWritesLock.entrySet()) {
			amountSpentLocally += entry.getValue();
		}
		return amountSpentLocally;
	}

	public void createLocks(String requesterId, Collection<Lock> locks) {
		beginTxn(null);
		for (Lock l : locks) {
			SharedLockCRDT lock = getLock(l.id(), true, true);
			if (lock != null)
				lock.updateOwnership(sequencer.siteId, requesterId, l.type());
		}
		endTxn(true);
	}

	public void createCounters(String requesterId, Collection<CounterReservation> counters) {
		beginTxn(null);
		for (CounterReservation c : counters) {
			LowerBoundCounterCRDT counter = getCounter(c.getId(), true, true);
			if (counter != null)
				counter.increment(c.getAmount(), requesterId);
		}
		endTxn(true);
	}

	_TxnHandle handle;

	void beginTxn(Timestamp cltTimestamp) {
		handle = new _TxnHandle(getCurrentClock(), cltTimestamp);
	}

	public void endTxn(final boolean writeThrough) {
		if (writeThrough)
			handle.commit();
	}

	class _TxnHandle extends AbstractTxHandle {

		_TxnHandle(CausalityClock snapshot, Timestamp cltTimestamp) {
			super(snapshot, cltTimestamp);
		}

		@Override
		public void commit() {
			try {
				Timings.mark();
				final CommitUpdatesRequest req;
				List<CRDTObjectUpdatesGroup<?>> updates = getUpdates();

				if (!updates.isEmpty()) {
					Timestamp ts = sequencer.clocks.newTimestamp();

					req = new CommitUpdatesRequest(LOCK_MANAGER + "-" + sequencer.siteId, cltTimestamp(), snapshot, updates);
					req.setTimestamps(ts, null);
				} else {
					req = new CommitUpdatesRequest(LOCK_MANAGER + "-" + sequencer.siteId, new Timestamp("dummy", -1L), snapshot, updates);
				}

				final Semaphore semaphore = new Semaphore(0);
				stub.asyncRequest(surrogate, req, (CommitUpdatesReply r) -> {
					if (Log.isLoggable(Level.INFO))
						Log.info("FINISH COMMIT------->>" + r.getStatus() + " FOR : " + req.getTimestamp());
					semaphore.release();
				});
				semaphore.acquireUninterruptibly();
			} finally {
				Timings.sample("async commit");
			}
		}
		@SuppressWarnings("unchecked")
		@Override
		protected <V extends CRDT<V>> ManagedCRDT<V> getCRDT(CRDTIdentifier uid, CausalityClock version, boolean create, Class<V> classOfV) {
			try {
				Timings.mark();
				FetchObjectRequest req = new FetchObjectRequest(LOCK_MANAGER, uid, true);

				Timings.mark();
				FetchObjectReply reply = stub.request(surrogate, req);
				Timings.sample("fetchCRDT(" + uid + ") from" + surrogate);

				if (reply != null) {
					if (reply.getStatus() == FetchObjectReply.FetchStatus.OK) {
						return (ManagedCRDT<V>) reply.getCrdt();
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

		public <V extends CRDT<V>> V getMostRecent(CRDTIdentifier id, boolean create, Class<V> classOfV) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {

			return (V) this.getCRDT(id, getCurrentClock(), create, classOfV).getLatestVersion(this);
		}

		protected Timestamp cltTimestamp() {
			if (cltTimestamp == null)
				cltTimestamp = cltTimestampSource.generateNew();

			return cltTimestamp;
		}
	}

}
