package swift.indigo;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
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
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesReply.AcquireReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.TransferResourcesRequest;
import swift.utils.Pair;
import sys.net.api.Endpoint;
import sys.utils.Threading;

final public class IndigoResourceManager {
    private static Logger logger = Logger.getLogger(IndigoResourceManager.class.getName());

    final String resourceMgrId;
    final public static String LOCKS_TABLE = "/indigo/locks";

    private final Map<Resource<?>, SortedSet<AcquireResourcesRequest>> pending;

    private final StorageHelper storage;
    private final IndigoSequencerAndResourceManager sequencer;

    private Map<CRDTIdentifier, Resource<?>> cache;

    private transient Queue<TransferResourcesRequest> transferQueue;

    private boolean isMaster;

    public IndigoResourceManager(IndigoSequencerAndResourceManager sequencer, Endpoint surrogate,
            Map<String, Endpoint> endpoints, Queue<TransferResourcesRequest> transferQueue) {

        // TODO: ENSURE ORDERING!
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

        this.cache = new HashMap<CRDTIdentifier, Resource<?>>();
        this.pending = new ConcurrentHashMap<Resource<?>, SortedSet<AcquireResourcesRequest>>();

        // this.pendingWrites = new HashMap<CRDTIdentifier, Map<Timestamp,
        // Integer>>();

        logger.info("ENDPOINTS: " + endpoints);

        this.storage.registerType(CounterReservation.class, NonNegativeBoundedCounterAsResource.class);
        this.storage.registerType(LockReservation.class, EscrowableTokenCRDT.class);

    }

    // This release modifies soft-state exclusively. If we want to support
    // durability this must be different
    public boolean releaseResources(AcquireResourcesReply alr) {
        boolean ok = true;
        try {
            alr.lockStuff();
            int cacheInt;
            for (ResourceRequest<?> req_i : alr.getResourcesRequest()) {
                if (req_i instanceof LockReservation) {
                    Resource<ShareableLock> resource = (Resource<ShareableLock>) cache.get(req_i.getResourceId());
                    ((EscrowableTokenCRDTWithLocks) resource).release(sequencer.siteId, req_i);
                }
                if (req_i instanceof CounterReservation) {
                    ConsumableResource<Integer> cachedResource = (ConsumableResource<Integer>) cache.get(req_i
                            .getResourceId());
                    cacheInt = cachedResource.getCurrentResource();
                    ((BoundedCounterWithLocalEscrow) cachedResource).release(sequencer.siteId, req_i);

                    // TODO: Warning this reads from storage, every time
                    // some notifications arrives.
                    // More efficient could be for instance to apply the
                    // decrement directly on the soft-state

                    try {
                        ConsumableResource<Integer> resource = (ConsumableResource<Integer>) storage.getResource(req_i);
                        int storageInt = resource.getCurrentResource();
                        resource = (ConsumableResource) updateLocalInfo(resource);

                        // System.out.println(req_i.getResourceId() +
                        // " Value from cache before release: " + cacheInt
                        // + " active: " + ((BoundedCounterWithLocalEscrow)
                        // cachedResource).getActiveresources()
                        // + " value from storage" + storageInt);

                    } catch (VersionNotFoundException e) {
                        logger.warning("Version exception but continues");
                    }
                }

            }

            // for (ResourceRequest<?> req_i : alr.getResourcesRequest()) {
            // checkPendingRequest(req_i);
            // }

        } catch (SwiftException e) {
            e.printStackTrace();
        } catch (IncompatibleTypeException e) {
            e.printStackTrace();
        } finally {
            alr.unlockStuff();
        }
        return ok || true; // TODO: handle cases for timestamps that do not
                           // involve
                           // locks...
    }

    AcquireResourcesReply acquireResources(AcquireResourcesRequest request) {
        Map<CRDTIdentifier, Resource<?>> unsatified = new HashMap<CRDTIdentifier, Resource<?>>();
        Map<CRDTIdentifier, Resource<?>> satisfiedFromStorage = new HashMap<CRDTIdentifier, Resource<?>>();

        AcquireResourcesRequest modifiedRequest = preProcessRequest(request);

        lockTable();
        CausalityClock snapshot = storage.getCurrentClock();
        try {
            storage.beginTxn(request.getClientTs());

            // Test resource's availability
            Resource resource;
            for (ResourceRequest<?> req : modifiedRequest.getRequests()) {
                try {
                    resource = storage.getResource(req);
                } catch (VersionNotFoundException e) {
                    System.out.println("Didn't found requested version, will deny message and continue");
                    return new AcquireResourcesReply(AcquireReply.NO, snapshot);
                }

                // This code is not actually being called, since the storage
                // getCRDT operation throws an exception when it reads a value
                // that does not exists
                if (resource == null)
                    resource = storage.createResource(req);

                // Updates local storage with the updates read from storage
                resource = updateLocalInfo(resource);
                boolean satisfies = resource.checkRequest(sequencer.siteId, req);
                if (!satisfies) {
                    unsatified.put(req.getResourceId(), resource);
                } else {
                    satisfiedFromStorage.put(req.getResourceId(), resource);
                }
            }
            // At this point every read is satisfied from soft-state

            // TODO: Asynchronous thread
            performProvisioning(request, request.getClientTs());

            if (unsatified.size() != 0) {
                return generateDenyMessage(unsatified, snapshot);
            }

            else {

                // Do the locking
                for (ResourceRequest<?> req : request.getRequests()) {
                    Resource resourceFromCache = cache.get(req.getResourceId());
                    resourceFromCache.apply(sequencer.siteId, req);
                }

                Collection<CRDTObjectUpdatesGroup<?>> updates = storage.endTxnAndGetUpdates(false);
                Timestamp txnTs = storage.recordNewEvent();

                return new AcquireResourcesReply(request.getClientTs(), txnTs, snapshot, updates, request.getRequests());
            }

        } catch (SwiftException e) {
            e.printStackTrace();
        } catch (IncompatibleTypeException e) {
            e.printStackTrace();
        } finally {
            unlockTable();
        }
        return generateDenyMessage(unsatified, snapshot);
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

    void checkPendingRequest(ResourceRequest<?> request) {
        SortedSet<AcquireResourcesRequest> pendingReqs = pending.remove(request);
        if (pendingReqs != null)
            for (AcquireResourcesRequest i : pendingReqs) {
                TRANSFER_STATUS result = transferResources(i);
                if (logger.isLoggable(Level.INFO))
                    logger.info("Check Pending: Transfer Resources: " + i + " Result: " + result);
                // TODO: Why stop here?
                // if (request.type.isExclusive())
                // break;
            }
    }

    TRANSFER_STATUS transferResources(final AcquireResourcesRequest request) {
        boolean allSuccess = true;
        boolean atLeastOnePartial = false;
        try {
            lockTable();
            boolean updated = false;
            storage.beginTxn(null);
            for (ResourceRequest<?> req_i : request.getRequests()) {
                Resource<?> resource = storage.getResource(req_i);
                TRANSFER_STATUS transferred = updateResourcesOwnership(req_i, resource);
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

            storage.endTxn(updated);
            if (allSuccess) {
                return TRANSFER_STATUS.SUCCESS;
            } else if (atLeastOnePartial) {
                return TRANSFER_STATUS.PARTIAL;
            } else {
                return TRANSFER_STATUS.FAIL;
            }

        } catch (SwiftException e) {
            e.printStackTrace();
        } finally {
            request.unlockStuff();
        }
        return TRANSFER_STATUS.FAIL;
    }

    private <T> TRANSFER_STATUS updateResourcesOwnership(ResourceRequest<?> request, Resource<T> resource)
            throws SwiftException {
        TRANSFER_STATUS result = TRANSFER_STATUS.FAIL;
        // An already satisfied request is handled as a failure. Cannot cache
        // replies because a request can arrive multiple times if it was partial
        if (!resource.checkRequest(request.getRequesterId(), (ResourceRequest<T>) request)) {
            ResourceRequest request_policy = transferPolicy(request, resource);
            if (request_policy != null) {
                request.lockStuff();
                resource = storage.getResource(request);
                TRANSFER_STATUS transferred = resource.transferOwnership(sequencer.siteId, request.getRequesterId(),
                        request_policy);
                request.unlockStuff();

                if (!request.equals(request_policy) && transferred.equals(TRANSFER_STATUS.SUCCESS)) {
                    result = TRANSFER_STATUS.PARTIAL;
                } else if (transferred.equals(TRANSFER_STATUS.SUCCESS)) {
                    result = TRANSFER_STATUS.SUCCESS;
                }
            }
        }
        return result;
    }

    // Checks what requests will be transferred
    private ResourceRequest<?> transferPolicy(ResourceRequest<?> request, Resource<?> resource) {
        return request;
    }

    /**
     * Verifies if the request is repeated. Checks what requests can be
     * satisfied with the local available information.
     * 
     * @param request
     *            the requested resources
     * @return the requests that cannot be satisfied using local information.
     */
    private AcquireResourcesRequest preProcessRequest(AcquireResourcesRequest request) {
        HashSet<ResourceRequest<?>> nonCached = new HashSet<ResourceRequest<?>>();

        for (ResourceRequest req : request.getRequests()) {
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
        for (ResourceRequest<?> req_i : req.getRequests()) {
            Resource resource = cache.get(req_i.getResourceId());
            if (!resource.checkRequest(sequencer.siteId, req_i)) {
                Queue<Pair<String, ?>> pref = resource.preferenceList();
                LinkedList<Pair<String, ResourceRequest<?>>> contactList = new LinkedList<Pair<String, ResourceRequest<?>>>();
                // TODO: ups... shortcut. Must abstract this and can make it
                // more elaborate.
                if (resource instanceof BoundedCounterWithLocalEscrow && pref.size() > 0) {
                    contactList.add(new Pair<String, ResourceRequest<?>>(pref.peek().getFirst(), req_i));
                }
                if (resource instanceof EscrowableTokenCRDTWithLocks && pref.size() > 0) {
                    if (((ShareableLock) req_i.getResource()).isShareable()) {
                        String preferred = pref.peek().getFirst();
                        if (!preferred.equals(sequencer.siteId))
                            contactList.add(new Pair<String, ResourceRequest<?>>(preferred, req_i));
                    } else {
                        for (Pair<String, ?> site : pref) {
                            if (!site.getFirst().equals(sequencer.siteId)) {
                                contactList.add(new Pair<String, ResourceRequest<?>>(site.getFirst(), req_i));
                            }
                        }
                    }
                }

                Pair<String, ResourceRequest<?>> transferFrom = contactList.peek();
                List<ResourceRequest<?>> list = requestsBySite.get(transferFrom.getFirst());
                if (list == null) {
                    requestsBySite.put(transferFrom.getFirst(), new LinkedList<ResourceRequest<?>>());
                    list = requestsBySite.get(transferFrom.getFirst());
                }
                list.add(transferFrom.getSecond());

            }
        }
        LinkedList<TransferResourcesRequest> returnList = new LinkedList<TransferResourcesRequest>();
        for (Entry<String, List<ResourceRequest<?>>> request : requestsBySite.entrySet()) {
            returnList.add(new TransferResourcesRequest(sequencer.siteId, request.getKey(), requestId, request
                    .getValue()));
        }
        return returnList;
    }

    private void lockTable() {
        Threading.lock(IndigoResourceManager.LOCKS_TABLE);
    }

    private void unlockTable() {
        Threading.unlock(IndigoResourceManager.LOCKS_TABLE);
    }

    public static ResourceRequest<?>[] sortedRequests(Collection<ResourceRequest<?>> requests) {
        int size = requests.size();
        return size == 0 ? new ResourceRequest[0] : new TreeSet<ResourceRequest<?>>(requests)
                .toArray(new ResourceRequest[size]);
    }
}
