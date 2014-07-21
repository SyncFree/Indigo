package swift.indigo;

import swift.api.CRDTIdentifier;

public interface Resource<T> {

    void initialize(String ownerId, ResourceRequest<T> request);

    TRANSFER_STATUS transferOwnership(String fromId, String toId, ResourceRequest<T> request);

    void apply(String siteId, ResourceRequest<T> req);

    CRDTIdentifier getUID();

    T getCurrentResource();

    T getSiteResource(String siteId);

    boolean isReservable();

    boolean checkRequest(String ownerId, ResourceRequest<T> request);

    boolean isOwner(String siteId);

}
