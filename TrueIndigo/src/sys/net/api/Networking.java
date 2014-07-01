package sys.net.api;

public interface Networking {

	static enum ServiceType {
		LOW_LATENCY, HIGH_CONTENTION
	};

	public Endpoint resolve(final String address);

	public Endpoint resolve(final String address, final String defaultAddress);

	public Service bind(Endpoint localEndpoint, MessageHandler handler);

	public Service bind(Endpoint localEndpoint, MessageHandler handler, ServiceType option);

	public Service stub();

	Service stub(ServiceType type);

}
