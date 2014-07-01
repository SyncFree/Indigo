package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.MessageHandler;
import sys.net.api.Service;
import sys.utils.IP;

public class Networking implements sys.net.api.Networking {
	static final ServiceType DEFAULT_SERVICE_TYPE = ServiceType.HIGH_CONTENTION;

	public static Networking impl() {
		return new Networking();
	}

	@Override
	public Endpoint resolve(String address) {
		address = address.replace("*", IP.localHostAddressString());
		return new NetworkingEndpoint(address);
	}

	@Override
	public Endpoint resolve(String address, String defaultAddress) {
		if (address != null || address.isEmpty()) {
			address = address.replace("*", IP.localHostAddressString());
			return new NetworkingEndpoint(address, defaultAddress);
		} else
			return resolve(defaultAddress);
	}

	@Override
	public Service bind(Endpoint localEndpoint, MessageHandler handler) {
		return new TcpService(localEndpoint, handler, DEFAULT_SERVICE_TYPE);
	}

	@Override
	public Service bind(Endpoint localEndpoint, MessageHandler handler, ServiceType type) {
		return new TcpService(localEndpoint, handler, type);
	}

	@Override
	public Service stub() {
		return new TcpService(DEFAULT_SERVICE_TYPE);
	}

	@Override
	public Service stub(ServiceType type) {
		return new TcpService(type);
	}

}
