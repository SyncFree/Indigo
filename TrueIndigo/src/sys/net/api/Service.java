package sys.net.api;

public interface Service {

	Endpoint localEndpoint();

	Service setDefaultTimeout(int ms);

	int getDefaultTimeout();

	<T> T request(final Endpoint dst, final Message m);

	<T> T request(final int retries, final Endpoint dst, final Message m);

	<T> void asyncRequest(final Endpoint dst, final Message m, Handler<T> replyHandler);

	<T> void asyncRequest(final Endpoint dst, final Message m, Handler<T> replyHandler, boolean streamingReplies);

	void send(final Endpoint dst, final Message m);
}
