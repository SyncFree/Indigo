package swift.indigo;

public interface ConsumableResource<T> extends Resource<T> {

    void produce(String ownerId, T req);

    boolean consume(String ownerId, ResourceRequest<T> req);

}
