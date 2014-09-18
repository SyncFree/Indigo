package swift.indigo;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.application.test.TestsUtil;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.BoundedCounterAsResource;
import swift.crdt.EscrowableTokenCRDTWithLocks;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTOperationDependencyPolicy;
import swift.crdt.core.ManagedCRDT;
import swift.exceptions.IncompatibleTypeException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.indigo.StorageHelper._TxnHandle;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesReply.AcquireReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.TransferResourcesRequest;
import swift.utils.Pair;
import sys.net.api.Endpoint;
import sys.utils.Args;
import sys.utils.Profiler;
import sys.utils.Threading;

final public class IndigoResourceManager {
	private Logger logger;

	private final String resourceMgrId;
	private final StorageHelper storage;
	private final IndigoSequencerAndResourceManager sequencer;

	private Map<CRDTIdentifier, ManagedCRDT<?>> cache;
	// private Map<CRDTIdentifier, Resource<?>> active;
	private Map<CRDTIdentifier, List<ResourceRequest<?>>> toBeReleased;
	private Map<CRDTIdentifier, ReentrantLock> lockTable;

	private Queue<TransferResourcesRequest> transferQueue;

	private final boolean isMaster;

	private int transferSeqNumber;

	private static String profilerName = "resource_manager";

	private static Profiler profiler;

	static final int DUPLICATE_TRANSFER_FILTER_WINDOW = 500;

	private static final int GRANT_THRESHOLD = 500;

	private static final int REQUEST_THRESHOLD = 100;

	ConcurrentHashMap<String, Long> recentTransfers = new ConcurrentHashMap<>();

	public IndigoResourceManager(IndigoSequencerAndResourceManager sequencer, Endpoint surrogate, Map<String, Endpoint> endpoints, Queue<TransferResourcesRequest> transferQueue) {
		this.transferQueue = transferQueue;
		this.sequencer = sequencer;
		this.resourceMgrId = sequencer.siteId + "-LockManager";

		if (!endpoints.isEmpty()) {
			String master = Args.valueOf("-master", new TreeSet<String>(endpoints.keySet()).first());
			this.isMaster = master.equals(sequencer.siteId);
		} else {
			this.isMaster = true;
		}
		this.storage = new StorageHelper(sequencer, surrogate, resourceMgrId, isMaster);

		this.cache = new ConcurrentHashMap<>();
		// this.active = new ConcurrentHashMap<>();
		this.lockTable = new ConcurrentHashMap<>();
		this.toBeReleased = new ConcurrentHashMap<>();
		logger = Logger.getLogger(IndigoResourceManager.class.getName());
		initLogger_dc();
	}

	private static void initLogger_dc() {
		Logger logger = Logger.getLogger(profilerName);
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

	protected void releaseResources(AcquireResourcesReply alr) {
		// for (ResourceRequest<?> res : alr.getResourcesRequest()) {
		// List<ResourceRequest<?>> list =
		// toBeReleased.get(res.getResourceId());
		// if (list == null) {
		// list = new LinkedList<>();
		// toBeReleased.put(res.getResourceId(), list);
		// }
		// synchronized (list) {
		// list.add(res);
		// }
		// }
	}

	// // This release modifies soft-state exclusively.
	// private boolean doReleaseResources(CRDTIdentifier id) {
	// boolean ok = false;
	// try {
	// getLock(id).lock();
	// _TxnHandle handle = storage.beginTxn(null);
	// Resource<?> resource = null;
	// List<ResourceRequest<?>> pendingReleases = toBeReleased.get(id);
	// if (pendingReleases != null) {
	// synchronized (pendingReleases) {
	// while (!pendingReleases.isEmpty()) {
	// ResourceRequest<?> req_i = pendingReleases.remove(0);
	// if (resource == null || resource.getUID() != req_i.getResourceId()) {
	// resource = getResourceAndUpdateCache(req_i, handle);
	// }
	// if (req_i instanceof LockReservation) {
	// ok = ((EscrowableTokenCRDTWithLocks) resource).release(sequencer.siteId,
	// req_i);
	// }
	// if (req_i instanceof CounterReservation) {
	// resource = getResourceAndUpdateCache(req_i, handle);
	// ok = ((BoundedCounterWithLocalEscrow) resource).release(sequencer.siteId,
	// req_i);
	// }
	//
	// }
	// }
	// }
	// } catch (VersionNotFoundException e) {
	// if (logger.isLoggable(Level.WARNING))
	// logger.warning("Version exception - did not finish releases " +
	// (++exceptionCount));
	// } catch (SwiftException e) {
	// e.printStackTrace();
	// } catch (IncompatibleTypeException e) {
	// e.printStackTrace();
	//
	// } finally {
	// getLock(id).unlock();
	// }
	// return ok || false;
	// }
	protected AcquireResourcesReply acquireResources(AcquireResourcesRequest request) {
		acquireLocks(request.getResources());
		Map<CRDTIdentifier, Resource<?>> unsatified = new HashMap<CRDTIdentifier, Resource<?>>();
		Map<CRDTIdentifier, Resource<?>> satisfiedFromStorage = new HashMap<CRDTIdentifier, Resource<?>>();
		Map<CRDTIdentifier, Resource<?>> active = new HashMap<>();

		// Only set this variable to true if request fails (no effect on the
		// client)
		CausalityClock snapshot = null;
		boolean mustUpdate = false;
		try {
			// lockTable();
			_TxnHandle handle = storage.beginTxn(request.getClientTs());
			snapshot = handle.snapshot;
			logger.info("Acquire started on  " + request.getClientTs() + " " + snapshot);
			AcquireResourcesRequest modifiedRequest = preProcessRequest(request, handle, active);
			// AcquireResourcesRequest modifiedRequest = request;
			// Test resource's availability
			Resource resource;
			for (ResourceRequest<?> req : modifiedRequest.getResources()) {
				try {
					resource = getResourceAndUpdateCache(req, handle, true);
					if (resource == null) {
						logger.warning("VALUE NOT AVAILABLE - aborting transaction");
						storage.endTxn(handle, mustUpdate);
						return new AcquireResourcesReply(AcquireReply.NO, snapshot);
					}
					active.put(resource.getUID(), resource);
				} catch (VersionNotFoundException e) {
					logger.warning("VersionException " + e.getMessage());
					storage.endTxn(handle, mustUpdate);
					return new AcquireResourcesReply(AcquireReply.NO, snapshot);
				} catch (java.lang.IllegalStateException e) {
					logger.warning("IllegalStateException - " + e.getMessage());
					storage.endTxn(handle, mustUpdate);
					return new AcquireResourcesReply(AcquireReply.NO, snapshot);
				}

				boolean satisfies = resource.checkRequest(sequencer.siteId, req);

				// If a resource cannot be satisfied, free it locally.
				// This is necessary to make the token converge
				if (!satisfies && !resource.isSingleOwner(sequencer.siteId) && resource.isOwner(sequencer.siteId) && req instanceof LockReservation) {
					resource.releaseShare(sequencer.siteId);
					satisfies = resource.checkRequest(sequencer.siteId, req);
					mustUpdate = true;
				}

				if (!satisfies) {
					unsatified.put(req.getResourceId(), resource);
				} else {
					satisfiedFromStorage.put(req.getResourceId(), resource);
				}
			}
			// At this point every read is satisfied from soft-state

			performProvisioning(request, request.getClientTs(), handle);

			if (unsatified.size() != 0) {
				printResourcesState(unsatified);
				storage.endTxnAndGetUpdates(handle, mustUpdate);
				return generateDenyMessage(unsatified, snapshot);
			}

			else {
				Timestamp txnTs = storage.recordNewEvent();
				// Execute the operations in soft-state
				for (ResourceRequest<?> req_i : request.getResources()) {
					if (req_i instanceof CounterReservation) {
						BoundedCounterAsResource latestVersion = (BoundedCounterAsResource) active.get(req_i.getResourceId());
						boolean result = latestVersion.decrement((int) req_i.getResource(), req_i.getRequesterId());
						CRDTObjectUpdatesGroup<BoundedCounterAsResource> updates = (CRDTObjectUpdatesGroup<BoundedCounterAsResource>) handle.ops.get(req_i.getResourceId());
						ManagedCRDT<BoundedCounterAsResource> cachedCRDT = (ManagedCRDT<BoundedCounterAsResource>) cache.get(req_i.getResourceId());
						if (result == false || updates == null || updates.getOperations().size() == 0) {
							System.out.println("UPDATES ZERO???? " + result + "/" + latestVersion.getClock() + "/" + latestVersion);
							System.exit(0);
						}
						updates.addSystemTimestamp(txnTs);
						cachedCRDT.execute(updates, CRDTOperationDependencyPolicy.IGNORE);
						// System.out.println("After applying " +
						// cachedCRDT.getClock());
						if (logger.isLoggable(Level.INFO))
							logger.info(cachedCRDT.getLatestVersion(handle) + " OBJ:" + cachedCRDT.getClock() + " SNAP:" + handle.snapshot + " LOCAL:" + storage.getLocalSnapshotClockCopy());
					}

				}

				storage.endTxn(handle, false);
				return new AcquireResourcesReply(request.getClientTs(), txnTs, snapshot, new LinkedList<CRDTObjectUpdatesGroup<?>>(), request.getResources());
			}

		} catch (SwiftException e) {
			e.printStackTrace();
		} catch (IncompatibleTypeException e) {
			e.printStackTrace();
		} finally {
			releaseLocks(request.getResources());
		}
		return generateDenyMessage(unsatified, snapshot);
	}
	private void printResourcesState(Map<CRDTIdentifier, Resource<?>> unsatified) {
		for (Entry<CRDTIdentifier, Resource<?>> un_i : unsatified.entrySet()) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Resouce could not be granted: " + un_i.getKey() + ": " + un_i.getValue());
		}

	}

	private AcquireResourcesReply generateDenyMessage(Map<CRDTIdentifier, Resource<?>> unsatified, CausalityClock snapshot) {
		boolean impossible = false;

		for (Entry<CRDTIdentifier, Resource<?>> entry : unsatified.entrySet()) {
			if (!entry.getValue().isReservable()) {
				impossible = true;
				break;
			}
		}
		return new AcquireResourcesReply(impossible ? AcquireReply.IMPOSSIBLE : AcquireReply.NO, snapshot);
	}

	// TODO:It seems that it does not block when no transference is executed
	// It might be due to this transaction
	protected TRANSFER_STATUS transferResources(final TransferResourcesRequest request) {
		acquireLocks(request.getResources());
		boolean allSuccess = true;
		boolean atLeastOnePartial = false;
		boolean updated = false;
		_TxnHandle handle = storage.beginTxn(null);
		try {
			for (ResourceRequest<?> req_i : request.getResources()) {
				TRANSFER_STATUS transferred = updateResourcesOwnership(req_i, handle);
				if (transferred.equals(TRANSFER_STATUS.FAIL)) {
					allSuccess = false;
				}
				if (transferred.equals(TRANSFER_STATUS.PARTIAL)) {
					atLeastOnePartial = true;
					allSuccess = false;
					updated = true;
				}
				if (transferred.equals(TRANSFER_STATUS.SUCCESS)) {
					atLeastOnePartial = true;
					updated = true;
				}
			}
		} catch (SwiftException e) {
			e.printStackTrace();
		} catch (IncompatibleTypeException e) {
			e.printStackTrace();
		} finally {
			storage.endTxn(handle, updated);
			// replay transaction operations in cache
			for (CRDTObjectUpdatesGroup<?> updates : handle.getUpdates()) {
				ManagedCRDT<BoundedCounterAsResource> crdt = (ManagedCRDT<BoundedCounterAsResource>) cache.get(updates.getTargetUID());
				updates.addSystemTimestamp(handle.commitTs);
				crdt.execute((CRDTObjectUpdatesGroup<BoundedCounterAsResource>) updates, CRDTOperationDependencyPolicy.IGNORE);
			}
			releaseLocks(request.getResources());
		}
		if (allSuccess) {
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
				// System.out.printf("Cached Value after merge %s with %s\n",
				// cachedValue, resourceCRDT);
			}
		} else {
			cachedValue = (ManagedCRDT<V>) cache.get(request.getResourceId());

		}
		if (cachedValue != null) {
			readClock.intersect(cachedValue.getClock());
			// readClock.merge(cachedValue.getPruneClock());
			// System.out.printf("Request %s %s, readClock %s, snapshot %s, cachedVersion %s, \n",
			// readFromStorage,
			// request, readClock, storage.getLocalSnapshotClockCopy(),
			// cachedValue.getClock());
			resource = (Resource<V>) cachedValue.getVersion(readClock, handle);

		}
		return resource;
	}
	private <T> TRANSFER_STATUS updateResourcesOwnership(ResourceRequest<?> request, _TxnHandle handle) throws SwiftException, IncompatibleTypeException {
		Resource resource = getResourceAndUpdateCache(request, handle, true);
		TRANSFER_STATUS result = TRANSFER_STATUS.FAIL;
		// If requests can be satisfied at the caller according the local
		// state, do not transfer... update is on the way
		String requestMsg = "";
		// if (!resource.checkRequest(request.getRequesterId(),
		// (ResourceRequest<T>) request)) {
		if (((int) resource.getSiteResource(request.getRequesterId())) - GRANT_THRESHOLD <= 0) {
			ResourceRequest request_policy = transferPolicy(request, resource);
			if (request_policy != null) {

				if (request_policy.compareTo(request) == 0) {
					requestMsg += "No transformation on the request ";
				} else {
					requestMsg += "Original request " + request + "; Transformed request: " + request_policy + " ";
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
					if (!resource.isSingleOwner(sequencer.siteId)) {
						resource.releaseShare(sequencer.siteId);
					}
				}

			} else {
				requestMsg += "No resources available ";
			}
		}
		if (logger.isLoggable(Level.INFO))
			logger.info(requestMsg + " " + result + " " + resource);
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
		for (ResourceRequest req : request.getResources()) {
			Resource<?> resource = getResourceAndUpdateCache(req, handle, false);
			if (resource == null || (!resource.checkRequest(sequencer.siteId, req))) {
				nonCached.add(req);
			} else {
				active.put(req.getResourceId(), resource);
			}
		}

		return new AcquireResourcesRequest(request.getClientId(), request.getClientTs(), nonCached);

	}
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
			// if (logger.isLoggable(Level.INFO))
			// logger.info("Checking permissions for " + resource +
			// " and request " + req);
			if (((int) resource.getSiteResource(sequencer.siteId)) - REQUEST_THRESHOLD <= 0) {
				Queue<Pair<String, ?>> pref = resource.preferenceList(sequencer.siteId);
				LinkedList<Pair<String, ResourceRequest<?>>> contactList = new LinkedList<Pair<String, ResourceRequest<?>>>();
				// TODO: ups... shortcut. Must abstract this and can make it
				// more elegant.
				if (resource instanceof BoundedCounterAsResource && pref.size() > 0) {
					contactList.add(new Pair<String, ResourceRequest<?>>(pref.peek().getFirst(), req_i));
				}
				if (resource instanceof EscrowableTokenCRDTWithLocks && pref.size() > 0) {
					LockReservation req_i_lock = (LockReservation) req_i;
					if ((req_i_lock.getResource()).isCompatible(((EscrowableTokenCRDTWithLocks) resource).getCurrentResource())) {
						String preferred = pref.peek().getFirst();
						contactList.add(new Pair<String, ResourceRequest<?>>(preferred, req_i));
					} else {
						for (Pair<String, ?> site : pref) {
							contactList.add(new Pair<String, ResourceRequest<?>>(site.getFirst(), req_i));
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
		}
		LinkedList<TransferResourcesRequest> returnList = new LinkedList<TransferResourcesRequest>();
		for (Entry<String, List<ResourceRequest<?>>> request : requestsBySite.entrySet()) {
			returnList.add(new TransferResourcesRequest(sequencer.siteId, request.getKey(), requestId, request.getValue(), transferSeqNumber++));
		}
		if (returnList.size() > 0 && logger.isLoggable(Level.INFO)) {
			logger.info("Will get permissions from: " + returnList);
		}
		return returnList;
	}

}
