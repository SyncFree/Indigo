package sys.net.api;

public interface Envelope {

	Endpoint sender();

	<T> void reply(T msg);

	int msgSize();
}
