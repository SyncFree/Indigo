package swift.indigo;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
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

    private Queue<TransferResourcesRequest> transferQueue;

    private Endpoint masterLockManager;
    private boolean isMaster;

    private Map<String, Endpoint> endpoints;

    public IndigoResourceManager(IndigoSequencerAndResourceManager sequencer, Map<String, Endpoint> endpoints,
            Queue<TransferResourcesRequest> transferQueue) {

        // TODO: ADD ORDERING!
        this.transferQueue = transferQueue;
        this.sequencer = sequencer;
        this.resourceMgrId = sequencer.siteId + "-LockManager";

        if (!endpoints.isEmpty()) {
            String master = new TreeSet<String>(endpoints.keySet()).first();
            this.masterLockManager = endpoints.get(master);
            this.isMaster = master.equals(sequencer.siteId);
        } else {
            this.isMaster = true;
        }
        this.storage = new StorageHelper(sequencer, resourceMgrId, isMaster);

        this.cache = new HashMap<CRDTIdentifier, Resource<?>>();
        this.pending = new ConcurrentHashMap<Resource<?>, SortedSet<AcquireResourcesRequest>>();

        // this.pendingWrites = new HashMap<CRDTIdentifier, Map<Timestamp,
        // Integer>>();

        this.endpoints = endpoints;

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
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IncompatibleTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            alr.unlockStuff();
        }
        return ok || true; // TODO: handle cases for timestamps that do not
                           // involve
                           // locks...
    }

    AcquireResourcesReply acquireResources(AcquireResourcesRequest request) {

        // TODO: This is wrong: If client sends the same timestamp, request is
        // always the same
        Map<CRDTIdentifier, Resource<?>> unsatified = new HashMap<CRDTIdentifier, Resource<?>>();
        Map<CRDTIdentifier, Resource<?>> satifiedFromStorage = new HashMap<CRDTIdentifier, Resource<?>>();

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
                    System.out.println("Didn't found requested version, will denny message and continue");
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
                    satifiedFromStorage.put(req.getResourceId(), resource);
                }
            }
            // At this point every read is satisfied from soft-state

            // TODO: Asynchronous thread
            performProvisioning(request);

            if (unsatified.size() != 0) {
                return generateDennyMessage(unsatified, snapshot);
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
        return generateDennyMessage(unsatified, snapshot);
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

    private AcquireResourcesReply generateDennyMessage(Map<CRDTIdentifier, Resource<?>> unsatified,
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
                transferResources(i);
                // TODO: Why stop here?
                // if (request.type.isExclusive())
                // break;
            }
    }

    // TRANSFER_STATUS updateOwnershipForRemoteDC(AcquireResourcesRequest
    // request) {
    // if (logger.isLoggable(Level.INFO))
    // logger.info("PROCESSING updateOwnershipForRemoteDC " + request);
    //
    // try {
    // request.lockStuff();
    //
    // boolean updated = false;
    // storage.beginTxn(null);
    // for (LockReservation lock : request.locks()) {
    // SharedLockCRDT2 l = storage.getLock(lock.getResourceId(), false, false);
    // // Log.info("READ LOCK:" + l + "   Owners :" + l.owners());
    // if (l == null || l.alreadyUpdated(request.requesterId(),
    // lock.getResource()))
    // continue;
    //
    // if (updateResourceOwnership(request, lock)) {
    // pending.remove(lock);
    // updated = true;
    // } else {
    // SortedSet<AcquireLocksRequest> pendingReqs = pending.get(lock);
    // if (pendingReqs == null)
    // pending.put(lock, pendingReqs = new TreeSet<AcquireLocksRequest>());
    // pendingReqs.add(new AcquireLocksRequest(request.requesterId(), lock));
    // }
    // }
    // storage.endTxn(updated);
    // } finally {
    // request.unlockStuff();
    // }
    // // Log.info("PROCESS EXIT: " + updated + "---->>>>>>" +
    // // handle.getUpdates().size());
    // }

    TRANSFER_STATUS transferResources(final AcquireResourcesRequest request) {
        TRANSFER_STATUS success = TRANSFER_STATUS.FAIL;
        try {
            lockTable();
            boolean updated = false;
            storage.beginTxn(null);
            for (ResourceRequest<?> req_i : request.getRequests()) {
                Resource<?> resource = storage.getResource(req_i);
                TRANSFER_STATUS transferred = updateResourcesOwnership(req_i, resource);
                if (transferred == TRANSFER_STATUS.SUCCESS && success == TRANSFER_STATUS.FAIL) {
                    success = TRANSFER_STATUS.SUCCESS;
                } else if (transferred == TRANSFER_STATUS.PARTIAL) {
                    success = TRANSFER_STATUS.PARTIAL;
                }
            }

            storage.endTxn(updated);
            // System.err.println("1START TRANSFER");
            // final LowerBoundCounterCRDT counter =
            // getCounter(request.getCounterId(), false);
            // System.err.println(counter);
            // TODO: abstract provisioning rules.
            // int localAvailable = counter.availableSiteId(sequencer.siteId) -
            // amountSpentLocally(request.getCounterId());

            // int alreadyTransfered = counter.givenTo(sequencer.siteId,
            // request.getTargetSite());
            // int remoteAvailable =
            // counter.availableSiteId(request.getTargetSite());

            // System.err.println(request.getCounterId() +
            // "##### 2   BEFORE:    LOCAL:" + localAvailable + " REMOTE:"
            // + remoteAvailable + "  RECEIVED: " + alreadyTransfered + "   " +
            // counter);
            // if (remoteAvailable <= alreadyTransfered / 3 && localAvailable /
            // 3 > 0) {
            // if (logger.isLoggable(Level.INFO))
            // logger.info("Grant " + (localAvailable / 3) + " permissions to "
            // + request.getTargetSite());
            //
            // updated = counter.transfer(localAvailable / 3, sequencer.siteId,
            // request.getTargetSite());
            // System.err.println("Grant " + updated + " " + (localAvailable
            // / 3) + " permissions to "
            // + request.getTargetSite() + " LA " +
            // counter.availableSiteId(sequencer.siteId) + " " + counter
            // + " " + amountSpentLocally(request.getCounterId()));

            // }
            // localAvailable = counter.availableSiteId(sequencer.siteId) -
            // amountSpentLocally(request.getCounterId());

            // remoteAvailable =
            // counter.availableSiteId(request.getTargetSite());

            // System.err.println(request.getCounterId() +
            // "##### 3   AFTER:    LOCAL:" + localAvailable + " REMOTE:"
            // + remoteAvailable + "  RECEIVED: " + alreadyTransfered + "   " +
            // counter);

        } catch (SwiftException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            request.unlockStuff();
        }
        return success;
    }

    private TRANSFER_STATUS updateResourcesOwnership(ResourceRequest<?> request, Resource<?> resource)
            throws SwiftException {
        ResourceRequest request_policy = transferPolicy(request, resource);
        request.lockStuff();
        resource = storage.getResource(request);
        // TODO: Here requesterId should be the sequencer's
        TRANSFER_STATUS transferred = resource.transferOwnership(sequencer.siteId, request.getRequesterId(),
                request_policy);
        request.unlockStuff();
        return transferred;
    }

    // TODO: Must abstract transference and provisioning policies
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

    private void performProvisioning(AcquireResourcesRequest request) {
        for (ResourceRequest<?> req : request.getRequests()) {
            TransferResourcesRequest transferRequest = provisionPolicy(req);
            if (transferRequest != null) {
                transferQueue.add(transferRequest);
            }
        }

        // new Task(0) {
        // // Try to acquire ownership remotely for failed locks...
        // public void run() {
        // if (failedLocks.isEmpty())
        // return;
        //
        // if (logger.isLoggable(Level.INFO))
        // logger.info("FAILED TO ACQUIRE:" + failedLocks);
        //
        // if (!unknownLocks.isEmpty() || !unknownCounters.isEmpty()) {
        // cltEndpoint.send(masterLockManager, new
        // InitializeResources(sequencer.siteId, unknownLocks,
        // unknownCounters), RpcHandler.NONE, 0);
        // }
        //
        // Map<String, Set<LockReservation>> owners = new HashMap<String,
        // Set<LockReservation>>();
        // for (LockReservation lock : failedLocks) {
        // SharedLockCRDT2 l = storage.getLock(lock.getResourceId(), false,
        // false);
        // // Only fetches the lock if it no longer has ownership
        // if (!l.isOwner(sequencer.siteId)) {
        // l = storage.getLock(lock.getResourceId(), true, true);
        // }
        //
        // if (logger.isLoggable(Level.INFO))
        // logger.info("WANT LOCK: " + lock.resourceId + "--->" + l);
        // if (l != null) {
        // for (String i : l.owners()) {
        // Set<LockReservation> lockSet = owners.get(i);
        // if (lockSet == null)
        // owners.put(i, lockSet = new HashSet<LockReservation>());
        // lockSet.add(lock);
        //
        // if (lock.type.isShareable())
        // break;
        // }
        // }
        // }
        // if (logger.isLoggable(Level.INFO))
        // logger.info("ASKING:" + owners.entrySet());
        // for (Map.Entry<String, Set<LockReservation>> e : owners.entrySet()) {
        // AcquireLocksRequest req = new AcquireLocksRequest(sequencer.siteId,
        // e.getValue());
        // String ownerId = e.getKey();
        // if (!ownerId.equals(sequencer.siteId))
        // cltEndpoint.send(endpoints.get(ownerId), req);
        // }
        // }
        // };
        //
        // new Task(0) {
        // // Try to transfer reservations to the local manager
        // public void run() {
        // if (logger.isLoggable(Level.INFO))
        // logger.info("FAILED TO RESERVE COUNTERS: " + failedCounters);
        //
        // for (LowerBoundCounterCRDT c : failedCounters) {
        // TransferReservationRequest req = new
        // TransferReservationRequest(sequencer.siteId, c.getUID());
        // // Updates the preference list
        // LowerBoundCounterCRDT counter = storage.getCounter(c.getUID(),
        // false);
        // Queue<Pair<String, Integer>> preferred = counter.preferenceList();
        //
        // int available = counter.availableSiteId(sequencer.siteId);
        //
        // if (logger.isLoggable(Level.INFO))
        // logger.info("-------->available:" + available);
        //
        // if (available <= 0) {
        // if (logger.isLoggable(Level.INFO))
        // logger.info("preferred list " + preferred + " ENDPOINTS " +
        // endpoints);
        //
        // Endpoint endpoint = endpoints.get(preferred.peek().getFirst());
        // if (endpoint != null) {
        // cltEndpoint.send(endpoint, req);
        // } else {
        // if (logger.isLoggable(Level.INFO))
        // logger.warning("No endpoint for manager @ " +
        // preferred.peek().getFirst());
        // }
        // }
        //
        // }
        // }
        // };
    }

    private TransferResourcesRequest provisionPolicy(ResourceRequest<?> req) {
        // TODO Auto-generated method stub
        return null;
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
