package sys.net.impl;

import sys.net.api.Endpoint;

public interface ServiceChannel {

	boolean send(final Object m);

	Endpoint localEndpoint();

	Endpoint remoteEndpoint();

	void setRemoteGID(GID gid);
}