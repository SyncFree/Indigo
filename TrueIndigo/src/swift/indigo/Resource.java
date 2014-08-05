package swift.indigo;

import java.util.Collection;
import java.util.Queue;

import swift.api.CRDTIdentifier;
import swift.utils.Pair;

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

    public Queue<Pair<String, T>> preferenceList();

    Collection<String> getAllResourceOwners();

}
