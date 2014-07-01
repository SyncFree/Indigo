package test;

import static sys.Context.Networking;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Threading;

public class Server implements Runnable {

	@Override
	public void run() {
		Endpoint localEndpoint = Networking.resolve("tcp://*:1234");
		Service srv = Networking.bind(localEndpoint, new TestProtocol() {
			@Override
			public void onReceive(Envelope e, Probe p) {
				e.reply(p);
			}
		});

		System.err.printf("Server ready [ %s ]\n", srv.localEndpoint());
		Threading.sleep(1000000);
		System.err.println("Server main thread terminated...");
	}

	public static void main(String[] args) throws Exception {
		new Server().run();
	}

}
