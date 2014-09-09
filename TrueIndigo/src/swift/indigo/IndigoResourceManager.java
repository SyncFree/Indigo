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

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.BoundedCounterAsResource;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.EscrowableTokenCRDTWithLocks;
import swift.crdt.NonNegativeBoundedCounterAsResource;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
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

	private Map<CRDTIdentifier, ManagedCRDT<?>> cache;
	private Map<CRDTIdentifier, List<ResourceRequest<?>>> toBeReleased;
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
		this.toBeReleased = new ConcurrentHashMap<>();

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

	protected void releaseResources(AcquireResourcesReply alr) {
		for (ResourceRequest<?> res : alr.getResourcesRequest()) {
			List<ResourceRequest<?>> list = toBeReleased.get(res.getResourceId());
			if (list == null) {
				list = new LinkedList<>();
				toBeReleased.put(res.getResourceId(), list);
			}
			synchronized (list) {
				list.add(res);
			}
		}
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

		String append = "";

		Map<CRDTIdentifier, Resource<?>> unsatified = new HashMap<CRDTIdentifier, Resource<?>>();
		Map<CRDTIdentifier, Resource<?>> satisfiedFromStorage = new HashMap<CRDTIdentifier, Resource<?>>();

		// Only set this variable to true if request fails (no effect on the
		// client)
		CausalityClock snapshot = null;
		boolean mustUpdate = false;
		try {
			// lockTable();
			_TxnHandle handle = storage.beginTxn(request.getClientTs());
			snapshot = storage.getCurrentClock();
			AcquireResourcesRequest modifiedRequest = preProcessRequest(request, handle);
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

			performProvisioning(request, request.getClientTs(), handle);

			if (unsatified.size() != 0) {
				printResourcesState(unsatified);
				storage.endTxnAndGetUpdates(handle, mustUpdate);
				return generateDenyMessage(unsatified, snapshot);
			}

			else {

				// Do the locking
				for (ResourceRequest<?> req : request.getResources()) {
					ManagedCRDT<?> crdt = cache.get(req.getResourceId());
					Resource resourceFromCache = (Resource) cache.get(req.getResourceId()).getLatestVersion(handle);
					// logger.info("Satisfy request " + req + " for resource " +
					// resourceFromCache);
					resourceFromCache.apply(sequencer.siteId, req);
					append = "" + crdt;
				}

				Collection<CRDTObjectUpdatesGroup<?>> updates = storage.endTxnAndGetUpdates(handle, false);
				Timestamp txnTs = storage.recordNewEvent();
				System.out.println("-----------------------------" + txnTs + " " + append);

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

	private <V extends CRDT<V>> Resource<?> updateLocalInfoAndGetResource(ManagedCRDT<V> resourceCRDT, _TxnHandle handle)
			throws IncompatibleTypeException {
		// Resource<?> updated = null;
		Resource<?> resource = (Resource<?>) resourceCRDT.getLatestVersion(handle);
		ManagedCRDT<V> cachedValue = (ManagedCRDT<V>) cache.get(resource.getUID());
		if (cachedValue == null) {
			cache.put(resourceCRDT.getUID(), resourceCRDT);
			cachedValue = resourceCRDT;
		} else {
			cachedValue.merge(resourceCRDT);
		}
		return (Resource<?>) cachedValue.getLatestVersion(handle);
		// TODO: UPS... hide this! And use factory
		// if (resource instanceof BoundedCounterAsResource) {
		// if (cachedValue == null) {
		// updated =
		// BoundedCounterWithLocalEscrow.createDecorator(sequencer.siteId,
		// (BoundedCounterAsResource) resource);
		// } else {
		// updated = ((BoundedCounterWithLocalEscrow) cachedValue)
		// .createDecoratorCopy((Resource<Integer>) resource);
		// }
		// }
		// if ((resource.getLatestVersion(handle) instanceof
		// EscrowableTokenCRDT) {
		// if (cachedValue == null) {
		// updated = EscrowableTokenCRDTWithLocks
		// .createDecorator(sequencer.siteId, (EscrowableTokenCRDT) resource);
		// } else {
		// updated = ((EscrowableTokenCRDTWithLocks) cachedValue)
		// .createDecoratorCopy((Resource<ShareableLock>) resource);
		// }
		// }
		// return updated;
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
		ManagedCRDT<?> resource = storage.getResource(request, handle);
		// Updates local storage with the updates read from storage
		return updateLocalInfoAndGetResource(resource, handle);
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
			logger.info(requestMsg + " " + result + " " + resource);
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
			if (((BoundedCounterAsResource) resource).checkRequest(sequencer.siteId, counterReq))
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
	 * @throws SwiftException
	 * @throws IncompatibleTypeException
	 */
	private AcquireResourcesRequest preProcessRequest(AcquireResourcesRequest request, _TxnHandle handle)
			throws SwiftException, IncompatibleTypeException {
		HashSet<ResourceRequest<?>> nonCached = new HashSet<ResourceRequest<?>>();

		for (ResourceRequest req : request.getResources()) {
			ManagedCRDT<?> cachedCRDT = cache.get(req.getResourceId());
			if (cachedCRDT == null) {
				nonCached.add(req);
			} else if (!((Resource<?>) cachedCRDT.getLatestVersion(handle)).checkRequest(sequencer.siteId, req)) {
				ManagedCRDT<?> resourceCRDT = storage.getResource(req, handle);
				updateLocalInfoAndGetResource(resourceCRDT, handle);
				if (!((Resource<?>) cachedCRDT.getLatestVersion(handle)).checkRequest(sequencer.siteId, req))
					nonCached.add(req);
			}
		}

		return new AcquireResourcesRequest(request.getClientId(), request.getClientTs(), nonCached);

	}
	private void performProvisioning(AcquireResourcesRequest request, Timestamp ts, _TxnHandle handle) {
		List<TransferResourcesRequest> transferRequests = provisionPolicy(request, ts, handle);
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
	private List<TransferResourcesRequest> provisionPolicy(AcquireResourcesRequest req, Timestamp requestId,
			_TxnHandle handle) {
		Map<String, List<ResourceRequest<?>>> requestsBySite = new HashMap<String, List<ResourceRequest<?>>>();
		for (ResourceRequest<?> req_i : req.getResources()) {
			Resource resource = (Resource) cache.get(req_i.getResourceId()).getLatestVersion(handle);
			// if (logger.isLoggable(Level.INFO))
			// logger.info("Checking permissions for " + resource +
			// " and request " + req);
			if (!resource.checkRequest(sequencer.siteId, req_i)) {
				Queue<Pair<String, ?>> pref = resource.preferenceList(sequencer.siteId);
				LinkedList<Pair<String, ResourceRequest<?>>> contactList = new LinkedList<Pair<String, ResourceRequest<?>>>();
				// TODO: ups... shortcut. Must abstract this and can make it
				// more elegant.
				if (resource instanceof BoundedCounterAsResource && pref.size() > 0) {
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
					logger.info("Request " + req + " is already satisfied: " + resource
							+ ((BoundedCounterAsResource) resource).getClock());
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
