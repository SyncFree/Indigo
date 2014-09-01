package sys.shepard;

import static sys.Context.Networking;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.shepard.proto.GrazingGranted;
import sys.shepard.proto.GrazingRequest;
import sys.shepard.proto.ShepardProtoHandler;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Tasks;

/**
 * 
 * Simple manager for coordinating execution of multiple swiftsocial instances
 * in a single deployment...
 * 
 * @author smduarte
 * 
 */
public class PatientShepard implements ShepardProtoHandler {

	private static Logger Log = Logger.getLogger(PatientShepard.class.getName());
	private static final String DEFAULT_SHEPARD_URL = "tcp://*:29876";

	private Service stub;
	private int count;
	private ConcurrentHashMap<Endpoint, Envelope> waitingSheep;
	private int automaticShutdown;

	public static void sheepJoinHerd(String shepardUrl) {
		Endpoint shepard = Networking.resolve(shepardUrl, DEFAULT_SHEPARD_URL);
		Service stub = Networking.stub();

		stub.setDefaultTimeout(300 * 1000);
		Log.info("Contacting shepard at: " + shepardUrl);

		final Semaphore barrier = new Semaphore(0);

		stub.asyncRequest(shepard, new GrazingRequest(), (GrazingGranted reply) -> {
			barrier.release();

			Tasks.exec(reply.duration(), () -> {
				Log.warning(IP.localHostAddressString() + " Terminating after shepard timeout!!...");
				System.exit(0);
			});
		});
		barrier.acquireUninterruptibly();
		Log.info(IP.localHostAddressString() + " Let's GO!!!!!");
	}

	public PatientShepard() {
		int nSheeps = Args.valueOf("-count", 1);
		this.automaticShutdown = Args.valueOf("-duration", 180000);
		this.count = nSheeps;
		this.waitingSheep = new ConcurrentHashMap<Endpoint, Envelope>();

		Endpoint localEndpoint = Networking.resolve(Args.valueOf("-url", DEFAULT_SHEPARD_URL));
		stub = Networking.bind(localEndpoint, this);
		System.out.printf("SHEPARD: Ready and waiting for %d sheeps @ %s\n", nSheeps, stub.localEndpoint());
	}

	public static void main(String[] args) {
		Args.use(args);
		new PatientShepard();
	}

	public synchronized void onReceive(Envelope sheep, GrazingRequest request) {
		if (waitingSheep.put(sheep.sender(), sheep) == null) {
			if (--count > 0) {
				System.out.printf("SHEPARD: Added one more sheep: %s, total: %d sheep\n", sheep.sender(),
						waitingSheep.size());
			} else {
				System.out.printf("SHEPARD: All sheeps have joined.\n");
				releaseSheep();

			}
		}
	}

	void releaseSheep() {
		System.err.printf("SHEPARD: Releasing: %d sheep\n", waitingSheep.size());

		Set<Endpoint> released = new HashSet<Endpoint>();
		waitingSheep.forEach((e, sheep) -> {
			sheep.reply(new GrazingGranted(automaticShutdown, 0));
			released.add(e);
		});
		System.exit(0);
	}
}
