package sys.net.impl;

import sys.net.api.Endpoint;
import sys.net.api.MessageHandler;

abstract public class ServiceEndpoint {

	protected final TcpService service;
	protected final MessageHandler msghandler;

	protected ServiceEndpoint(MessageHandler msghandler, TcpService service) {
		this.msghandler = msghandler;
		this.service = service;
	}

	abstract public void connect(Endpoint remote);

	abstract public Endpoint localEndpoint();
}
