package sys.net.api;

public interface Handler<T> {
	void deliver(T msg);
}
