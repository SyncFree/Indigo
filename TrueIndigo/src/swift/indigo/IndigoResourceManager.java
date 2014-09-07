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
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.BoundedCounterAsResource;
import swift.crdt.BoundedCounterWithLocalEscrow;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.EscrowableTokenCRDTWithLocks;
import swift.crdt.NonNegativeBoundedCounterAsResource;
import swift.crdt.ShareableLock;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.exceptions.IncompatibleTypeException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.indigo.StorageHelper._TxnHandle;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesReply.AcquireReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.TransferResourcesRequest;
import swift.utils.LogSiteFormatter;
import swift.utils.Pair;
import sys.net.api.Endpoint;

final public class IndigoResourceManager {
	private Logger logger;

	private final String resourceMgrId;
	private final StorageHelper storage;
	private final IndigoSequencerAndResourceManager sequencer;

	private Map<CRDTIdentifier, Resource<?>> cache;
	private Map<CRDTIdentifier, ReentrantLock> lockTable;

	private transient Queue<TransferResourcesRequest> transferQueue;

	private boolean isMaster;

	private int transferSeqNumber;

	private int exceptionCount;

	public IndigoResourceManager(IndigoSequencerAndResourceManager sequencer, Endpoint surrogate,
			Map<String, Endpoint> endpoints, Queue<TransferResourcesRequest> transferQueue) {

		this.transferQueue = transferQueue;
		this.sequencer = sequencer;
		this.resourceMgrId = sequencer.siteId + "-LockManager";

		if (!endpoints.isEmpty()) {
			String master = new TreeSet<String>(endpoints.keySet()).first();
			this.isMaster = master.equals(sequencer.siteId);
		} else {
			this.isMaster = true;
		}
		this.storage = new StorageHelper(sequencer, surrogate, resourceMgrId, isMaster);

		this.cache = new ConcurrentHashMap<>();
		this.lockTable = new ConcurrentHashMap<>();

		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new LogSiteFormatter(sequencer.siteId));
		logger = Logger.getLogger(this.getClass().getName() + "." + sequencer.siteId);
		logger.setUseParentHandlers(false);
		logger.addHandler(handler);

		if (logger.isLoggable(Level.INFO))
			logger.info("ENDPOINTS: " + endpoints);

		this.storage.registerType(CounterReservation.class, NonNegativeBoundedCounterAsResource.class);
		this.storage.registerType(LockReservation.class, EscrowableTokenCRDT.class);

	}
	// This release modifies soft-state exclusively. If we want to support
	// durability this must be different
	protected boolean releaseResources(AcquireResourcesReply alr) {
		boolean ok = false;
		try {
			acquireLocks(alr.getResourcesRequest());

			// TODO: Warning this reads from storage, every time
			// some notifications arrives.
			// More efficient could be for instance to apply the
			// decrement directly on the soft-state

			_TxnHandle handle = storage.beginTxn(null);

			for (ResourceRequest<?> req_i : alr.getResourcesRequest()) {
				if (req_i instanceof LockReservation) {
					Resource<ShareableLock> resource = (Resource<ShareableLock>) getResourceAndUpdateCache(req_i,
							handle);
					ok = ((EscrowableTokenCRDTWithLocks) resource).release(sequencer.siteId, req_i);
				}
				if (req_i instanceof CounterReservation) {
					ConsumableResource<Integer> cachedResource = (ConsumableResource<Integer>) getResourceAndUpdateCache(
							req_i, handle);
					ok = ((BoundedCounterWithLocalEscrow) cachedResource).release(sequencer.siteId, req_i);
				}

			}

			// for (ResourceRequest<?> req_i : alr.getResourcesRequest()) {
			// checkPendingRequest(req_i);
			// }

		} catch (VersionNotFoundException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("Version exception but continues " + (++exceptionCount));
		} catch (SwiftException e) {
			e.printStackTrace();
		} catch (IncompatibleTypeException e) {
			e.printStackTrace();

		} finally {
			releaseLocks(alr.getResourcesRequest());
		}
		return ok || false;
	}

	protected AcquireResourcesReply acquireResources(AcquireResourcesRequest request) {
		acquireLocks(request.getResources());

		Map<CRDTIdentifier, Resource<?>> unsatified = new HashMap<CRDTIdentifier, Resource<?>>();
		Map<CRDTIdentifier, Resource<?>> satisfiedFromStorage = new HashMap<CRDTIdentifier, Resource<?>>();

		AcquireResourcesRequest modifiedRequest = preProcessRequest(request);
		// Only set this variable to true if request fails (no effect on the
		// client)
		boolean mustUpdate = false;
		CausalityClock snapshot = storage.getCurrentClock();
		try {
			// lockTable();
			_TxnHandle handle = storage.beginTxn(request.getClientTs());

			// Test resource's availability
			Resource resource;
			for (ResourceRequest<?> req : modifiedRequest.getResources()) {
				try {
					resource = getResourceAndUpdateCache(req, handle);
				} catch (VersionNotFoundException e) {
					logger.warning("VersionException " + e.getMessage());
					storage.endTxn(handle, mustUpdate);
					return new AcquireResourcesReply(AcquireReply.NO, snapshot);
				}

				boolean satisfies = resource.checkRequest(sequencer.siteId, req);

				// If a resource cannot be satisfied, free it locally.
				// This is necessary to make the token converge
				if (!satisfies && !resource.isSingleOwner(sequencer.siteId) && resource.isOwner(sequencer.siteId)) {
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

			performProvisioning(request, request.getClientTs());

			if (unsatified.size() != 0) {
				printResourcesState(unsatified);
				storage.endTxnAndGetUpdates(handle, mustUpdate);
				return generateDenyMessage(unsatified, snapshot);
			}

			else {

				// Do the locking
				for (ResourceRequest<?> req : request.getResources()) {
					Resource resourceFromCache = cache.get(req.getResourceId());
					// logger.info("Satisfy request " + req + " for resource " +
					// resourceFromCache);
					resourceFromCache.apply(sequencer.siteId, req);
				}

				Collection<CRDTObjectUpdatesGroup<?>> updates = storage.endTxnAndGetUpdates(handle, false);
				Timestamp txnTs = storage.recordNewEvent();

				return new AcquireResourcesReply(request.getClientTs(), txnTs, snapshot, updates,
						request.getResources());
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

	private Resource<?> updateLocalInfo(Resource<?> resource) throws IncompatibleTypeException {
		Resource<?> updated = null;
		Resource<?> cachedValue = cache.get(resource.getUID());
		// TODO: UPS... hide this! And use factory
		if (resource instanceof BoundedCounterAsResource) {
			if (cachedValue == null) {
				updated = BoundedCounterWithLocalEscrow.createDecorator(sequencer.siteId,
						(BoundedCounterAsResource) resource);
			} else {
				updated = ((BoundedCounterWithLocalEscrow) cachedValue)
						.createDecoratorCopy((Resource<Integer>) resource);
			}
		}
		if (resource instanceof EscrowableTokenCRDT) {
			if (cachedValue == null) {
				updated = EscrowableTokenCRDTWithLocks
						.createDecorator(sequencer.siteId, (EscrowableTokenCRDT) resource);
			} else {
				updated = ((EscrowableTokenCRDTWithLocks) cachedValue)
						.createDecoratorCopy((Resource<ShareableLock>) resource);
			}
		}
		cache.put(resource.getUID(), updated);
		return updated;
	}

	private AcquireResourcesReply generateDenyMessage(Map<CRDTIdentifier, Resource<?>> unsatified,
			CausalityClock snapshot) {
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
			System.out.println("UPD " + updated + " SUCC " + allSuccess + " " + request);
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
			ReentrantLock lock = getLock(req.getResourceId());
			lock.lock();
		}
	}

	private void releaseLocks(Collection<ResourceRequest<?>> resourcesRequest) {
		for (ResourceRequest<?> req : resourcesRequest) {
			ReentrantLock lock = lockTable.get(req.getResourceId());
			lock.unlock();
		}
	}

	private ReentrantLock getLock(CRDTIdentifier resourceId) {
		// TODO: Add ordering to locks so that they do not create a deadlock
		ReentrantLock lock = lockTable.get(resourceId);
		if (lock == null) {
			lock = new ReentrantLock();
			ReentrantLock result = lockTable.putIfAbsent(resourceId, lock);
			if (result == null) {
				lock = lockTable.get(resourceId);
			}
		}
		return lock;
	}

	private Resource<?> getResourceAndUpdateCache(ResourceRequest<?> request, _TxnHandle handle) throws SwiftException,
			IncompatibleTypeException {
		Resource<?> resource = storage.getResource(request, handle);
		// Updates local storage with the updates read from storage
		resource = updateLocalInfo(resource);
		return resource;
	}

	private <T> TRANSFER_STATUS updateResourcesOwnership(ResourceRequest<?> request, _TxnHandle handle)
			throws SwiftException, IncompatibleTypeException {
		Resource resource = getResourceAndUpdateCache(request, handle);
		TRANSFER_STATUS result = TRANSFER_STATUS.FAIL;
		// If requests can be satisfied at the caller according the local
		// state, do not transfer... update is on the way
		String requestMsg = "";
		if (!resource.checkRequest(request.getRequesterId(), (ResourceRequest<T>) request)) {
			ResourceRequest request_policy = transferPolicy(request, resource);
			if (request_policy != null) {

				if (request_policy.compareTo(request) == 0) {
					requestMsg += "No transformation on the request ";
				} else {
					requestMsg += "Original request " + request + "; Transformed request: " + request_policy + " ";
				}

				TRANSFER_STATUS transferred = resource.transferOwnership(sequencer.siteId, request.getRequesterId(),
						request_policy);

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
			logger.info(requestMsg + " " + result);
		return result;
	}
	// For Locks retrieve the same request
	// For counters retrieves half of the available resources. Returns null if
	// is impossible to transfer anything
	private ResourceRequest<?> transferPolicy(ResourceRequest<?> request, Resource<?> resource) {
		if (request instanceof CounterReservation) {
			CounterReservation counterReq = (CounterReservation) request;
			int availableLocally = (int) resource.getCurrentResource();
			int requested = counterReq.getResource();
			// If request can be satisfied retrieve max(request ,
			// half-permission)
			if (((BoundedCounterWithLocalEscrow) resource).checkRequest(sequencer.siteId, counterReq))
				request = new CounterReservation(request.getRequesterId(), request.getResourceId(), Math.max(
						availableLocally / 2, requested));
			else {
				// If cannot be satisfied, transfer half of the available
				if (availableLocally / 2 > 0)
					request = new CounterReservation(request.getRequesterId(), request.getResourceId(),
							availableLocally / 2);
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
	 * @return the requests that cannot be satisfied using local information.
	 */
	private AcquireResourcesRequest preProcessRequest(AcquireResourcesRequest request) {
		HashSet<ResourceRequest<?>> nonCached = new HashSet<ResourceRequest<?>>();

		for (ResourceRequest req : request.getResources()) {
			Resource<?> cached = cache.get(req.getResourceId());
			if (cached == null || !cached.checkRequest(sequencer.siteId, req)) {
				nonCached.add(req);
			}
		}

		return new AcquireResourcesRequest(request.getClientId(), request.getClientTs(), nonCached);

	}

	private void performProvisioning(AcquireResourcesRequest request, Timestamp ts) {
		List<TransferResourcesRequest> transferRequests = provisionPolicy(request, ts);
		// TODO: Not a very smart "contains" check - should look for requests
		// for the same keys
		if (transferRequests.size() > 0 && !transferQueue.contains(transferRequests)) {
			transferQueue.addAll(transferRequests);
		}
	}

	/**
	 * Request permissions for all resources not available locally.
	 * 
	 * @param req
	 * @param requestId
	 * @return
	 */
	private List<TransferResourcesRequest> provisionPolicy(AcquireResourcesRequest req, Timestamp requestId) {
		Map<String, List<ResourceRequest<?>>> requestsBySite = new HashMap<String, List<ResourceRequest<?>>>();
		for (ResourceRequest<?> req_i : req.getResources()) {
			Resource resource = cache.get(req_i.getResourceId());
			// if (logger.isLoggable(Level.INFO))
			// logger.info("Checking permissions for " + resource +
			// " and request " + req);
			if (!resource.checkRequest(sequencer.siteId, req_i)) {
				Queue<Pair<String, ?>> pref = resource.preferenceList(sequencer.siteId);
				LinkedList<Pair<String, ResourceRequest<?>>> contactList = new LinkedList<Pair<String, ResourceRequest<?>>>();
				// TODO: ups... shortcut. Must abstract this and can make it
				// more elegant.
				if (resource instanceof BoundedCounterWithLocalEscrow && pref.size() > 0) {
					contactList.add(new Pair<String, ResourceRequest<?>>(pref.peek().getFirst(), req_i));
				}
				if (resource instanceof EscrowableTokenCRDTWithLocks && pref.size() > 0) {
					LockReservation req_i_lock = (LockReservation) req_i;
					if ((req_i_lock.getResource()).isCompatible(((EscrowableTokenCRDTWithLocks) resource)
							.getCurrentResource())) {
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

			} else {
				if (logger.isLoggable(Level.INFO))
					logger.info("Request " + req + " is already satisfied");
			}
		}
		LinkedList<TransferResourcesRequest> returnList = new LinkedList<TransferResourcesRequest>();
		for (Entry<String, List<ResourceRequest<?>>> request : requestsBySite.entrySet()) {
			returnList.add(new TransferResourcesRequest(sequencer.siteId, request.getKey(), requestId, request
					.getValue(), transferSeqNumber++));
		}
		if (returnList.size() > 0 && logger.isLoggable(Level.INFO)) {
			logger.info("Will get permissions from: " + returnList);
		}
		return returnList;
	}

}
