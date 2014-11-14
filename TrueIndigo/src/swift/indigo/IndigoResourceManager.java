package swift.indigo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.application.test.TestsUtil;
import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.ReturnableTimestampSourceDecorator;
import swift.clocks.Timestamp;
import swift.crdt.BoundedCounterAsResource;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.LocalLock;
import swift.crdt.ShareableLock;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTOperationDependencyPolicy;
import swift.crdt.core.ManagedCRDT;
import swift.exceptions.IncompatibleTypeException;
import swift.exceptions.SwiftException;
import swift.indigo.StorageHelper._TxnHandle;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesReply.AcquireReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.TransferResourcesRequest;
import swift.utils.LogSiteFormatter;
import swift.utils.Pair;
import sys.net.api.Endpoint;
import sys.utils.Args;
import sys.utils.Profiler;
import sys.utils.Threading;

final public class IndigoResourceManager {
	private static Logger logger;

	private final String resourceMgrId;
	private final StorageHelper storage;
	private final IndigoSequencerAndResourceManager sequencer;

	private Map<CRDTIdentifier, ManagedCRDT<?>> cache;
	private Map<CRDTIdentifier, LocalLock> locks;
	private Map<CRDTIdentifier, Set<Pair<ResourceRequest<ShareableLock>, Timestamp>>> toBeReleased;

	private Queue<TransferResourcesRequest> transferQueue;

	private final boolean isMaster;

	protected AtomicInteger transferSeqNumber;

	private static String profilerName = "resource_manager";

	private static Profiler profiler;

	static final int DUPLICATE_TRANSFER_FILTER_WINDOW = 500;

	private static final int GRANT_THRESHOLD = 500;

	private static final int REQUEST_THRESHOLD = 100;

	ConcurrentHashMap<String, Long> recentTransfers = new ConcurrentHashMap<>();

	private String masterId;

	private ReturnableTimestampSourceDecorator<Timestamp> tsSource;

	private Map<CRDTIdentifier, ResourceRequest<?>> pendingRequests;

	public IndigoResourceManager(IndigoSequencerAndResourceManager sequencer, Endpoint surrogate, Map<String, Endpoint> endpoints, Queue<TransferResourcesRequest> transferQueue, Map<CRDTIdentifier, ResourceRequest<?>> pendingRequests) {
		this.transferQueue = transferQueue;
		this.pendingRequests = pendingRequests;
		this.sequencer = sequencer;
		this.resourceMgrId = sequencer.siteId + "-LockManager";

		if (!endpoints.isEmpty()) {
			this.masterId = Args.valueOf("-master", new TreeSet<String>(endpoints.keySet()).first());
			this.isMaster = masterId.equals(sequencer.siteId);
		} else {
			this.isMaster = true;
		}
		this.storage = new StorageHelper(sequencer, surrogate, resourceMgrId, isMaster);

		this.cache = new ConcurrentHashMap<>();
		this.locks = new ConcurrentHashMap<>();
		this.toBeReleased = new ConcurrentHashMap<>();
		this.tsSource = new ReturnableTimestampSourceDecorator<Timestamp>(new IncrementalTimestampGenerator(resourceMgrId));
		this.transferSeqNumber = new AtomicInteger();
		initLogger();
	}

	private void initLogger() {
		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new LogSiteFormatter(sequencer.siteId));
		logger = Logger.getLogger(this.getClass().getName() + "." + sequencer.siteId);
		logger.setUseParentHandlers(false);
		logger.addHandler(handler);

		profiler = Profiler.getInstance();
		if (logger.isLoggable(Level.FINEST)) {
			FileHandler fileTxt;
			try {
				String resultsDir = Args.valueOf("-results_dir", ".");
				String siteId = Args.valueOf("-siteId", "GLOBAL");
				String suffix = Args.valueOf("-fileNameSuffix", "");
				fileTxt = new FileHandler(resultsDir + "/resource_manager_log" + "_" + siteId + suffix + ".log");
				fileTxt.setFormatter(new java.util.logging.Formatter() {
					@Override
					public String format(LogRecord record) {
						return record.getMessage() + "\n";
					}
				});
				logger.addHandler(fileTxt);
				profiler.printMessage(profilerName, TestsUtil.dumpArgs());
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		profiler.printHeaderWithCustomFields(profilerName, "MSG");

	}

	@SuppressWarnings("unchecked")
	protected void releaseResources(AcquireResourcesReply alr) {
		acquireLocks(alr.getResourcesRequest());
		for (ResourceRequest<?> req : alr.getResourcesRequest()) {
			Set<Pair<ResourceRequest<ShareableLock>, Timestamp>> queue = toBeReleased.get(req.getResourceId());
			if (queue == null) {
				queue = createQueue();
				toBeReleased.put(req.getResourceId(), queue);
			}
			queue.add(new Pair<ResourceRequest<ShareableLock>, Timestamp>((ResourceRequest<ShareableLock>) req, alr.timestamp()));
		}
		releaseLocks(alr.getResourcesRequest());
	}

	private void doReleaseResources(CRDTIdentifier crdtId, CausalityClock durableSnapshot) {
		LocalLock lock = locks.get(crdtId);
		Set<Pair<ResourceRequest<ShareableLock>, Timestamp>> waiting = toBeReleased.get(crdtId);
		if (waiting != null) {
			Iterator<Pair<ResourceRequest<ShareableLock>, Timestamp>> it = waiting.iterator();
			while (it.hasNext()) {
				Pair<ResourceRequest<ShareableLock>, Timestamp> elem = it.next();
				if (durableSnapshot.includes(elem.getSecond())) {
					lock.release(sequencer.siteId, elem.getSecond());
					it.remove();
				}
			}
		}
	}
	private Set<Pair<ResourceRequest<ShareableLock>, Timestamp>> createQueue() {
		Set<Pair<ResourceRequest<ShareableLock>, Timestamp>> queue = new TreeSet<Pair<ResourceRequest<ShareableLock>, Timestamp>>(new Comparator<Pair<ResourceRequest<ShareableLock>, Timestamp>>() {
			@Override
			public int compare(Pair<ResourceRequest<ShareableLock>, Timestamp> o1, Pair<ResourceRequest<ShareableLock>, Timestamp> o2) {
				int res = o1.getSecond().compareTo(o2.getSecond());
				if (res == 0) {
					res = o1.getFirst().compareTo(o2.getFirst());
				}
				return res;
			}
		});
		return queue;
	}

	protected AcquireResourcesReply acquireResources(AcquireResourcesRequest request) {
		acquireLocks(request.getResources());
		Map<CRDTIdentifier, Resource<?>> unsatisfied = new HashMap<CRDTIdentifier, Resource<?>>();
		Map<CRDTIdentifier, Resource<?>> satisfiedFromStorage = new HashMap<CRDTIdentifier, Resource<?>>();
		Map<CRDTIdentifier, Resource<?>> active = new HashMap<>();

		// Only set this variable to true if request fails (no effect on the
		// client)
		CausalityClock snapshot = null;
		boolean mustUpdate = false;
		try {
			_TxnHandle handle = storage.beginTxn(tsSource.generateNew());
			snapshot = handle.snapshot;
			AcquireResourcesRequest modifiedRequest = preProcessRequest(request, handle, active);

			// Test resource's availability
			Resource resource;
			for (ResourceRequest<?> req : modifiedRequest.getResources()) {
				resource = getResourceAndUpdateCache(req, handle, true);
				active.put(resource.getUID(), resource);
				try {
					// If lock is being used locally it cannot be acquired.
					LocalLock localLock = null;
					if (req instanceof LockReservation) {
						localLock = locks.get(req.getResourceId());
						if (localLock != null && !localLock.checkAvailable((ShareableLock) req.getResource())) {
							if (localLock.checkCanRelease()) {
								boolean released = resource.releaseShare(sequencer.siteId, masterId);
								if (released) {
									mustUpdate = true;
								}
							}

							unsatisfied.put(req.getResourceId(), resource);
							continue;
						}
					}
				} catch (Exception e) {
					if (logger.isLoggable(Level.WARNING)) {
						logger.warning("Exception while retriving object from storage. Abort acquire " + e.getMessage());
					}
					storage.endTxn(handle, mustUpdate);
					return new AcquireResourcesReply(AcquireReply.NO, snapshot);
				}

				boolean satisfies = resource.checkRequest(sequencer.siteId, req);

				// If a resource cannot be satisfied, free it locally.
				// This is necessary to make the token converge
				if (req instanceof LockReservation && locks.get(req.getResourceId()).checkCanRelease() && !satisfies) {
					boolean released = resource.releaseShare(sequencer.siteId, masterId);
					satisfies = resource.checkRequest(sequencer.siteId, req);
					if (released) {
						mustUpdate = true;
					}
				}

				if (!satisfies) {
					logger.log(Level.WARNING, "Couldn't satisfy  " + resource + " RESOURCE CLOCK: " + ((CRDT<?>) resource).getClock() + " " + req + " " + locks.get(req.getResourceId()));
					unsatisfied.put(req.getResourceId(), resource);
				} else {
					satisfiedFromStorage.put(req.getResourceId(), resource);
				}
			}
			// At this point every read is satisfied from soft-state

			// Provision only after applying the updates - Locks may not be
			// satisfied before the update and be satisfied afterwards
			performProvisioning(request, request.getClientTs(), handle);

			if (unsatisfied.size() != 0) {
				printResourcesState(unsatisfied);
				// This only occurs for locks --- Can't we put the replaying of
				// operations in the finally?
				if (mustUpdate) {
					Timestamp txnTs = storage.recordNewEvent();
					for (CRDTObjectUpdatesGroup<?> updates : handle.getUpdates()) {
						ManagedCRDT<BoundedCounterAsResource> cachedCRDT = (ManagedCRDT<BoundedCounterAsResource>) cache.get(updates.getTargetUID());
						updates.addSystemTimestamp(txnTs);
						cachedCRDT.execute((CRDTObjectUpdatesGroup<BoundedCounterAsResource>) updates, CRDTOperationDependencyPolicy.IGNORE);
						cachedCRDT.getClock().recordAllUntil(handle.cltTimestamp());
					}
					storage.endTxnAndGetUpdates(handle, mustUpdate);
				}

				return generateDenyMessage(unsatisfied, snapshot);
			}

			else {
				Timestamp txnTs = storage.recordNewEvent();
				boolean result = false;
				Collection<CRDTObjectUpdatesGroup<?>> updatesToClient = new ArrayList<CRDTObjectUpdatesGroup<?>>();;

				// Execute the operations in soft-state
				for (ResourceRequest<?> req_i : request.getResources()) {
					if (req_i instanceof LockReservation) {
						LocalLock lock = locks.get(req_i.getResourceId());
						lock.lock(txnTs, (ShareableLock) req_i.getResource(), snapshot);
						EscrowableTokenCRDT cachedResource = (EscrowableTokenCRDT) active.get(req_i.getResourceId());
						result = cachedResource.updateOwnership(sequencer.siteId, req_i.getRequesterId(), ((LockReservation) req_i).type).equals(TRANSFER_STATUS.SUCCESS);
						updatesToClient.addAll((Collection<CRDTObjectUpdatesGroup<?>>) handle.getUpdates());
						CRDTObjectUpdatesGroup<BoundedCounterAsResource> updates = (CRDTObjectUpdatesGroup<BoundedCounterAsResource>) handle.ops.get(req_i.getResourceId());
						ManagedCRDT<BoundedCounterAsResource> cachedCRDT = (ManagedCRDT<BoundedCounterAsResource>) cache.get(req_i.getResourceId());
						updates.addSystemTimestamp(txnTs);
						cachedCRDT.execute(updates, CRDTOperationDependencyPolicy.IGNORE);
						cachedCRDT.getClock().recordAllUntil(handle.cltTimestamp());
					}
					if (req_i instanceof CounterReservation) {
						BoundedCounterAsResource latestVersion = (BoundedCounterAsResource) active.get(req_i.getResourceId());
						result = latestVersion.decrement((int) req_i.getResource(), req_i.getRequesterId());
						CRDTObjectUpdatesGroup<BoundedCounterAsResource> updates = (CRDTObjectUpdatesGroup<BoundedCounterAsResource>) handle.ops.get(req_i.getResourceId());
						ManagedCRDT<BoundedCounterAsResource> cachedCRDT = (ManagedCRDT<BoundedCounterAsResource>) cache.get(req_i.getResourceId());
						if (result == false || updates == null || updates.getOperations().size() == 0) {
							System.out.println("UPDATES ZERO???? " + result + "/" + latestVersion.getClock() + "/" + latestVersion);
							Thread.dumpStack();
							System.exit(0);
						}
						updates.addSystemTimestamp(txnTs);
						cachedCRDT.execute(updates, CRDTOperationDependencyPolicy.IGNORE);
						cachedCRDT.getClock().recordAllUntil(handle.cltTimestamp());
					}
					if (result == false) {
						System.out.println("FAILED RESOURCE UPDATE");
						Thread.dumpStack();
						System.exit(0);
					}
				}

				storage.endTxn(handle, false);
				return new AcquireResourcesReply(request.getClientTs(), txnTs, snapshot, updatesToClient, request.getResources());
			}

		} catch (SwiftException e) {
			e.printStackTrace();
		} catch (IncompatibleTypeException e) {
			e.printStackTrace();
		} finally {
			releaseLocks(request.getResources());
		}
		return generateDenyMessage(unsatisfied, snapshot);
	}
	private void printResourcesState(Map<CRDTIdentifier, Resource<?>> unsatified) {
		for (Entry<CRDTIdentifier, Resource<?>> un_i : unsatified.entrySet()) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Resource could not be granted: " + un_i.getKey() + ": " + un_i.getValue());
		}

	}

	private AcquireResourcesReply generateDenyMessage(Map<CRDTIdentifier, Resource<?>> unsatified, CausalityClock snapshot) {
		boolean impossible = false;
		Collection<CRDTIdentifier> impossibleResources = new HashSet<>();
		for (Entry<CRDTIdentifier, Resource<?>> entry : unsatified.entrySet()) {
			if (!entry.getValue().isReservable()) {
				impossible = true;
				impossibleResources.add(entry.getValue().getUID());
			}
		}
		return new AcquireResourcesReply(impossible ? AcquireReply.IMPOSSIBLE : AcquireReply.NO, snapshot, impossibleResources);
	}

	protected TRANSFER_STATUS transferResources(final TransferResourcesRequest request) {
		acquireLocks(request.getResources());
		boolean allSuccess = true;
		boolean atLeastOnePartial = false;
		boolean alreadySatisfied = false;
		boolean updated = false;
		_TxnHandle handle = storage.beginTxn(null);
		try {
			for (ResourceRequest<?> req_i : request.getResources()) {
				TRANSFER_STATUS transferred = updateResourcesOwnership(req_i, handle);
				if (transferred.equals(TRANSFER_STATUS.FAIL)) {
					allSuccess = false;
				}
				if (transferred.equals(TRANSFER_STATUS.ALREADY_SATISFIED)) {
					alreadySatisfied = true;
				}
				if (transferred.equals(TRANSFER_STATUS.PARTIAL)) {
					atLeastOnePartial = true;
					allSuccess = false;
					updated = true;
				}
				if (transferred.equals(TRANSFER_STATUS.SUCCESS)) {
					atLeastOnePartial = true;
					updated = true;
				} else if ((!transferred.equals(TRANSFER_STATUS.ALREADY_SATISFIED))) {
					synchronized (pendingRequests) {
						pendingRequests.put(req_i.getResourceId(), req_i);
					}
				}
			}
		} catch (SwiftException e) {
			e.printStackTrace();
		} catch (IncompatibleTypeException e) {
			e.printStackTrace();
		} finally {
			storage.endTxn(handle, updated);
			if (updated) {
				System.err.println(sequencer.siteId + ": transfer ---> " + handle.getUpdates().get(0).getTimestamps());
				// replay transaction operations in cache
				for (CRDTObjectUpdatesGroup<?> updates : handle.getUpdates()) {
					ManagedCRDT<BoundedCounterAsResource> crdt = (ManagedCRDT<BoundedCounterAsResource>) cache.get(updates.getTargetUID());
					updates.addSystemTimestamp(handle.commitTs);
					crdt.execute((CRDTObjectUpdatesGroup<BoundedCounterAsResource>) updates, CRDTOperationDependencyPolicy.IGNORE);
					crdt.getClock().recordAllUntil(handle.cltTimestamp());
				}
			}
			releaseLocks(request.getResources());
		}
		if (alreadySatisfied && !atLeastOnePartial && allSuccess) {
			return TRANSFER_STATUS.ALREADY_SATISFIED;
		} else if (allSuccess) {
			return TRANSFER_STATUS.SUCCESS;
		} else if (atLeastOnePartial) {
			return TRANSFER_STATUS.PARTIAL;
		} else {
			return TRANSFER_STATUS.FAIL;
		}
	}
	private void acquireLocks(Collection<ResourceRequest<?>> resourcesRequest) {
		for (ResourceRequest<?> req : resourcesRequest) {
			Threading.lock(req.getResourceId() + " " + sequencer.siteId);
		}
	}

	private void releaseLocks(Collection<ResourceRequest<?>> resourcesRequest) {
		for (ResourceRequest<?> req : resourcesRequest) {
			Threading.unlock(req.getResourceId() + " " + sequencer.siteId);
		}
	}

	private <V extends CRDT<V>> Resource<?> getResourceAndUpdateCache(ResourceRequest<?> request, _TxnHandle handle, boolean readFromStorage) throws SwiftException, IncompatibleTypeException {
		ManagedCRDT<V> cachedValue;
		Resource<V> resource = null;
		CausalityClock readClock = handle.readTimestamp;
		if (readFromStorage) {
			ManagedCRDT<V> resourceCRDT = (ManagedCRDT<V>) storage.getResource(request, handle);
			cachedValue = (ManagedCRDT<V>) cache.get(request.getResourceId());
			if (cachedValue == null && resourceCRDT != null) {
				cache.put(request.getResourceId(), resourceCRDT);
				cachedValue = resourceCRDT;
			} else {
				cachedValue.merge(resourceCRDT);
			}
		} else {
			cachedValue = (ManagedCRDT<V>) cache.get(request.getResourceId());

		}
		if (cachedValue != null) {
			readClock.intersect(cachedValue.getClock());
			readClock.merge(cachedValue.getPruneClock());
			if (request instanceof LockReservation) {
				LocalLock lock = locks.get(request.getResourceId());
				V resourceCRDT = cachedValue.getVersion(readClock, handle);
				if (lock == null) {
					locks.put(request.getResourceId(), new LocalLock());
				} else {
					doReleaseResources(request.getResourceId(), readClock);
				}
				return (Resource<V>) resourceCRDT;
			} else {
				return (Resource<V>) cachedValue.getVersion(readClock, handle);
			}
		}
		return null;
	}
	private <T> TRANSFER_STATUS updateResourcesOwnership(ResourceRequest<?> request, _TxnHandle handle) throws SwiftException, IncompatibleTypeException {
		Resource resource = getResourceAndUpdateCache(request, handle, true);
		TRANSFER_STATUS result = TRANSFER_STATUS.FAIL;
		// If requests can be satisfied at the caller according the local
		// state, do not transfer... update is on the way
		boolean needsTransference = false;
		if (request instanceof CounterReservation) {
			if (((int) resource.getSiteResource(request.getRequesterId())) - GRANT_THRESHOLD <= 0) {
				needsTransference = true;
			}
		} else if (request instanceof LockReservation) {
			doReleaseResources(request.getResourceId(), handle.snapshot);
			if (!resource.checkRequest(request.getRequesterId(), request)) {
				needsTransference = true;
			}
		}

		if (needsTransference) {
			ResourceRequest request_policy = transferPolicy(request, resource);
			if (request_policy != null) {

				// If request is a lock and it cannot be granted locally return
				// false;
				if (request instanceof LockReservation) {
					if (!locks.get(request.getResourceId()).checkAvailable((ShareableLock) request.getResource())) {
						return result;
					}
				}
				TRANSFER_STATUS transferred = resource.transferOwnership(sequencer.siteId, request.getRequesterId(), request_policy);

				// If the request from the policy is less than the original, is
				// just a partial request
				if (request_policy.compareTo(request) >= 0 && transferred.equals(TRANSFER_STATUS.SUCCESS)) {
					result = TRANSFER_STATUS.SUCCESS;
				} else if (transferred.equals(TRANSFER_STATUS.SUCCESS)) {
					result = TRANSFER_STATUS.PARTIAL;
				}

				// Transference failed - release the lock if it is not a single
				// owner.
				// This is implemented for locks, for counters it does not
				// release any
				// share.

				if (result.equals(TRANSFER_STATUS.FAIL)) {
					// If a resource cannot be satisfied, free it locally.
					// This is necessary to make the token converge
					if (request instanceof LockReservation && locks.get(request.getResourceId()).checkCanRelease()) {
						boolean released = resource.releaseShare(sequencer.siteId, masterId);
						if (released) {
							logger.warning("RELEASED " + resource + " REQUEST " + request);
							result = TRANSFER_STATUS.PARTIAL;
						}
					}
				}

			}
		} else {
			result = TRANSFER_STATUS.ALREADY_SATISFIED;
		}
		if (logger.isLoggable(Level.WARNING)) {
			logger.warning("Resource transfer: request: " + request + " resource: " + resource + " clock: " + ((CRDT<?>) resource).getClock() + " result: " + result);
		}
		return result;
	}
	// For Locks retrieve the same request
	// For counters retrieves half of the available resources. Returns null if
	// is impossible to transfer anything
	private ResourceRequest<?> transferPolicy(ResourceRequest<?> request, Resource<?> resource) {
		if (request instanceof CounterReservation) {
			CounterReservation counterReq = (CounterReservation) request;
			int availableLocally = (int) resource.getSiteResource(sequencer.siteId);
			int requested = counterReq.getResource();
			// If request can be satisfied retrieve max(request ,
			// half-permission)
			if (((BoundedCounterAsResource) resource).checkRequest(sequencer.siteId, counterReq))
				request = new CounterReservation(request.getRequesterId(), request.getResourceId(), Math.max(availableLocally / 2, requested));
			else {
				// If cannot be satisfied, transfer half of the available
				if (availableLocally / 2 > 0)
					request = new CounterReservation(request.getRequesterId(), request.getResourceId(), availableLocally / 2);
				else
					request = null;
			}
		}
		// All requests
		return request;
	}

	/**
	 * Checks what requests can be satisfied with the local available
	 * information.
	 * 
	 * @param request
	 *            the requested resources
	 * @param active
	 * @return the requests that cannot be satisfied using local information.
	 * @throws SwiftException
	 * @throws IncompatibleTypeException
	 */
	private AcquireResourcesRequest preProcessRequest(AcquireResourcesRequest request, _TxnHandle handle, Map<CRDTIdentifier, Resource<?>> active) throws SwiftException, IncompatibleTypeException {
		HashSet<ResourceRequest<?>> nonCached = new HashSet<ResourceRequest<?>>();
		for (ResourceRequest req_i : request.getResources()) {
			Resource<?> resource = getResourceAndUpdateCache(req_i, handle, false);
			if (req_i instanceof LockReservation) {
				// Release the resources that are committed globally
				doReleaseResources(req_i.getResourceId(), handle.snapshot);
				LocalLock lock = locks.get(req_i.getResourceId());
				if (lock != null && lock.checkAvailable((ShareableLock) req_i.getResource()) && resource.checkRequest(sequencer.siteId, req_i)) {
					active.put(req_i.getResourceId(), resource);
				} else {
					nonCached.add(req_i);
				}
			} else {
				if (resource == null || (!resource.checkRequest(sequencer.siteId, req_i))) {
					nonCached.add(req_i);
				} else {
					active.put(req_i.getResourceId(), resource);
				}
			}
		}

		return new AcquireResourcesRequest(request.getClientId(), request.getClientTs(), nonCached);

	}
	// TODO: request trasnference
	private void performProvisioning(AcquireResourcesRequest request, Timestamp ts, _TxnHandle handle) {
		Collection<ResourceRequest<?>> candidates = new LinkedList<>();
		for (ResourceRequest<?> req_i : request.getResources()) {
			String key = req_i.key();
			long now = System.currentTimeMillis();
			Long tts = recentTransfers.get(key);
			if (tts == null || (now - tts) > DUPLICATE_TRANSFER_FILTER_WINDOW) {
				recentTransfers.put(key, now);
				candidates.add(req_i);
			}
		}

		List<TransferResourcesRequest> transferRequests = provisionPolicy(new AcquireResourcesRequest(request.getClientId(), request.getClientTs(), candidates), ts, handle);
		// TODO: Not a very smart "contains" check - should look for requests
		// for the same keys

		synchronized (transferQueue) {
			transferQueue.addAll(transferRequests);
			Threading.notifyAllOn(transferQueue);
		}

	}
	/**
	 * Request permissions for all resources not available locally.
	 * 
	 * @param req
	 * @param requestId
	 * @return
	 */
	private List<TransferResourcesRequest> provisionPolicy(AcquireResourcesRequest req, Timestamp requestId, _TxnHandle handle) {
		Map<String, List<ResourceRequest<?>>> requestsBySite = new HashMap<String, List<ResourceRequest<?>>>();
		for (ResourceRequest<?> req_i : req.getResources()) {
			Resource resource = (Resource) cache.get(req_i.getResourceId()).getLatestVersion(handle);
			// TODO: ups... shortcut. Must abstract this.
			Queue<Pair<String, ?>> pref = resource.preferenceList(sequencer.siteId);
			LinkedList<Pair<String, ResourceRequest<?>>> contactList = new LinkedList<Pair<String, ResourceRequest<?>>>();
			if (req_i instanceof CounterReservation) {
				if (((int) resource.getSiteResource(sequencer.siteId)) - REQUEST_THRESHOLD <= 0) {
					if (resource instanceof BoundedCounterAsResource && pref.size() > 0) {
						contactList.add(new Pair<String, ResourceRequest<?>>(pref.peek().getFirst(), req_i));
					}
				}
			} else if (req_i instanceof LockReservation) {
				if (!resource.checkRequest(sequencer.siteId, req_i)) {
					if (resource instanceof EscrowableTokenCRDT && pref.size() > 0) {
						LockReservation req_i_lock = (LockReservation) req_i;
						if ((req_i_lock.getResource()).isCompatible(((EscrowableTokenCRDT) resource).getCurrentResource())) {
							String preferred = pref.peek().getFirst();
							contactList.add(new Pair<String, ResourceRequest<?>>(preferred, req_i));
						} else {
							for (Pair<String, ?> site : pref) {
								contactList.add(new Pair<String, ResourceRequest<?>>(site.getFirst(), req_i));
							}
						}
					}
				}
			}

			for (Pair<String, ResourceRequest<?>> remoteNodeRequest : contactList) {
				List<ResourceRequest<?>> list = requestsBySite.get(remoteNodeRequest.getFirst());
				if (list == null) {
					requestsBySite.put(remoteNodeRequest.getFirst(), new LinkedList<ResourceRequest<?>>());
					list = requestsBySite.get(remoteNodeRequest.getFirst());
				}
				list.add(remoteNodeRequest.getSecond());
			}

		}
		LinkedList<TransferResourcesRequest> returnList = new LinkedList<TransferResourcesRequest>();
		for (Entry<String, List<ResourceRequest<?>>> request : requestsBySite.entrySet()) {
			returnList.add(new TransferResourcesRequest(sequencer.siteId, request.getKey(), requestId, request.getValue(), transferSeqNumber.incrementAndGet()));
		}
		if (returnList.size() > 0 && logger.isLoggable(Level.INFO)) {
			logger.info("Will get permissions from: " + returnList);
		}
		return returnList;
	}
}
