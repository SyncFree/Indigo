package swift.indigo;

import java.util.Collection;

public interface ReservationsAlgorithm {

    public <T> boolean requestResourcePolicy(String requesterId, Collection<ResourceRequest<T>> resources);

    public <T> boolean releaseResourcePolicy(Collection<ResourceRequest<T>> resources);

}
