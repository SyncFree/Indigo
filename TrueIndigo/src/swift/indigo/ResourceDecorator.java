package swift.indigo;

import java.util.Collection;

import swift.api.CRDTIdentifier;
import swift.exceptions.IncompatibleTypeException;

public abstract class ResourceDecorator<V extends ResourceDecorator<V, T>, T> implements Resource<T> {

    Resource<T> originalResource;
    private CRDTIdentifier uid;

    public ResourceDecorator() {
    }

    public ResourceDecorator(CRDTIdentifier uid, Resource<T> resource) {
        this.originalResource = resource;
        this.uid = uid;
    }

    // ATTENTION: To do a merge between the local cache and the value read, it
    // should use some version,
    // because a different algorithm may have multiple versions of the decorated
    // resource.
    // In our algorithm that doesn't happen, but just in case...
    public abstract V createDecoratorCopy(Resource<T> resource) throws IncompatibleTypeException;

    public void initialize(String ownerId, ResourceRequest<T> request) {
        originalResource.initialize(ownerId, request);
    }

    public TRANSFER_STATUS transferOwnership(String fromId, String toId, ResourceRequest<T> request) {
        return originalResource.transferOwnership(fromId, toId, request);
    }

    public T getSiteResource(String siteId) {
        return originalResource.getSiteResource(siteId);
    }

    @Override
    public T getCurrentResource() {
        return originalResource.getCurrentResource();
    }

    @Override
    public CRDTIdentifier getUID() {
        return uid;
    }

    @Override
    public boolean isOwner(String siteId) {
        return originalResource.isOwner(siteId);
    }

    @Override
    public boolean isReservable() {
        return originalResource.isReservable();
    }

    public Collection<String> getAllResourceOwners() {
        return originalResource.getAllResourceOwners();
    }
}
