package swift.crdt;

import java.util.HashSet;
import java.util.Set;

import swift.api.CRDTIdentifier;
import swift.exceptions.IncompatibleTypeException;
import swift.indigo.ConsumableResource;
import swift.indigo.Resource;
import swift.indigo.ResourceDecorator;
import swift.indigo.ResourceRequest;
import swift.indigo.TRANSFER_STATUS;

/**
 * By design this class returns the values of the original Resource minus the
 * resources being used. Must think if this is the best solution
 * 
 * @author balegas
 * 
 * @param <V>
 */
public class BoundedCounterWithLocalEscrow extends ResourceDecorator<BoundedCounterWithLocalEscrow, Integer> implements
        ConsumableResource<Integer> {

    private Set<ResourceRequest<Integer>> activeRequests;
    private String localId;

    public BoundedCounterWithLocalEscrow() {
        super();
    }

    public BoundedCounterWithLocalEscrow(String localId, CRDTIdentifier uid, ConsumableResource<Integer> original) {
        super(uid, original);
        this.activeRequests = new HashSet<ResourceRequest<Integer>>();
        this.localId = localId;
    }

    private BoundedCounterWithLocalEscrow(String localId, CRDTIdentifier uid, ConsumableResource<Integer> resource,
            HashSet<ResourceRequest<Integer>> activeRequests) {
        this(localId, uid, resource);
        this.activeRequests = activeRequests;
    }

    @Override
    public void initialize(String ownerId, ResourceRequest<Integer> request) {
        super.initialize(ownerId, request);
    }

    @Override
    public void apply(String siteId, ResourceRequest<Integer> req) {
        consume(siteId, req);
    }

    @Override
    public void produce(String ownerId, Integer req) {
        System.out.println("NOT IMPLEMENTED");
        System.exit(0);
    }

    @Override
    public boolean consume(String ownerId, ResourceRequest<Integer> req) {
        if (checkRequest(ownerId, req)) {
            // if (activeRequests.contains(req)) {
            // System.out
            // .println("Attention - Consecutive requests by the same client without the first being freed - needs UID to distinguish between them");
            // System.exit(0);
            // }
            activeRequests.add(req);
            return true;
        }
        return false;
    }

    @Override
    public TRANSFER_STATUS transferOwnership(String fromId, String toId, ResourceRequest<Integer> request) {
        if (checkRequest(fromId, request)) {
            TRANSFER_STATUS success = super.transferOwnership(fromId, toId, request);
            if (success != TRANSFER_STATUS.SUCCESS) {
                System.err.println("BUG -  SHOULD SUCCEED - REMOVE THIS MESSAGE");
                System.exit(0);
            }
            return success;
        }
        return TRANSFER_STATUS.FAIL;
    }

    private int resourcesInUse() {
        int inUse = 0;
        for (ResourceRequest<Integer> req_i : activeRequests) {
            inUse += req_i.getResource();
        }

        return inUse;
    }

    @Override
    public boolean checkRequest(String ownerId, ResourceRequest<Integer> request) {
        Integer available = getSiteResource(ownerId);
        return available >= request.getResource();
    }

    @Override
    public boolean isOwner(String siteId) {
        return getSiteResource(siteId) > 0;
    }

    @Override
    public Integer getSiteResource(String siteId) {
        if (siteId.equals(localId)) {
            int stableResources = super.getSiteResource(siteId);
            int activeResources = resourcesInUse();

            if (stableResources - activeResources < 0) {
                System.err.println("EXIT!!! Value became negative");
                // System.exit(0);
            }

            return stableResources - activeResources;
        } else {
            return super.getSiteResource(siteId);
        }
    }

    @Override
    public Integer getCurrentResource() {
        int resources = super.getCurrentResource() - resourcesInUse();

        if (resources < 0) {
            System.err.println("EXIT!!! Value became negative");
            // System.exit(0);
        }

        return resources;
    }

    @Override
    public boolean isReservable() {
        return getCurrentResource() != 0;
    }

    @Override
    public BoundedCounterWithLocalEscrow createDecoratorCopy(Resource<Integer> resource)
            throws IncompatibleTypeException {
        BoundedCounterWithLocalEscrow newDecorator = new BoundedCounterWithLocalEscrow(localId, getUID(),
                (ConsumableResource<Integer>) resource, new HashSet<ResourceRequest<Integer>>(activeRequests));
        return newDecorator;
    }

    public static BoundedCounterWithLocalEscrow createDecorator(String callerId, ConsumableResource<Integer> resource) {
        return new BoundedCounterWithLocalEscrow(callerId, resource.getUID(), resource);
    }

    public void release(String siteId, ResourceRequest<?> req_i) {
        if (!activeRequests.remove(req_i)) {
            System.out.println("ERROR release did not remove a resource " + req_i);
            System.exit(0);
        }
    }

    @Override
    public String toString() {
        return getCurrentResource() + "";
    }

    public String getActiveresources() {
        int out = 0;
        for (ResourceRequest<Integer> req : activeRequests) {
            out += req.getResource();
        }
        return out + "";
    }

}