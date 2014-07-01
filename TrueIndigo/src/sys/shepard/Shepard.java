package sys.shepard;

import static sys.Context.Networking;
import static sys.Context.Sys;

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
import sys.utils.Threading;

/**
 * 
 * Simple manager for coordinating execution of multiple swiftsocial instances
 * in a single deployment...
 * 
 * @author smduarte
 * 
 */
public class Shepard implements ShepardProtoHandler {
	private static Logger Log = Logger.getLogger(Shepard.class.getName());

	private static final int GRACE_PERIOD = 15000;
	private static final String DEFAULT_SHEPARD_URL = "tcp://*:29876";

	public static void sheepJoinHerd(String shepardUrl) {
		Endpoint shepard = Networking.resolve(shepardUrl, DEFAULT_SHEPARD_URL);
		Service stub = Networking.stub();

		stub.setDefaultTimeout(300 * 1000);
		System.err.println("Contacting shepard at: " + shepardUrl);

		final Semaphore barrier = new Semaphore(0);

		stub.asyncRequest(shepard, new GrazingRequest(), (GrazingGranted reply) -> {
			System.err.println("Got from shepard; when:" + reply.when() + "/ duration:" + reply.duration());
			Threading.sleep(reply.when() + Sys.random().nextInt(1000));
			barrier.release();

			Tasks.exec(reply.duration(), () -> {
				Log.info(IP.localHostAddressString() + " Meh...I'm done...");
				System.exit(0);
			});
		});
		barrier.acquireUninterruptibly();
		Log.info(IP.localHostAddressString() + " Let's GO!!!!!");
		System.err.println(IP.localHostAddressString() + " Let's GO!!!!!");
	}

	int totalSheep;
	int grazingDuration;

	Service stub;
	volatile boolean releasedSheep;
	volatile long releaseDeadline;
	ConcurrentHashMap<Endpoint, Envelope> waitingSheep;

	public Shepard() {
		this.releasedSheep = false;
		this.releaseDeadline = Long.MAX_VALUE;
		this.grazingDuration = Args.valueOf("-duration", 30);
		this.waitingSheep = new ConcurrentHashMap<Endpoint, Envelope>();

		Endpoint localEndpoint = Networking.resolve(Args.valueOf("-url", DEFAULT_SHEPARD_URL));
		stub = Networking.bind(localEndpoint, this);
		System.err.printf("SHEPARD: Ready...@ %s\n", stub.localEndpoint());
	}

	public static void main(String[] args) {
		Args.use(args);
		new Shepard();
	}

	public void onReceive(Envelope sheep, GrazingRequest request) {
		if (waitingSheep.put(sheep.sender(), sheep) == null)
			System.err.printf("SHEPARD: Added one more sheep: %s, total: %d sheep\n", sheep.sender(), waitingSheep.size());

		if (releasedSheep)
			releaseSheep();
		else {
			releaseDeadline = Sys.timeMillis() + GRACE_PERIOD;
			Tasks.exec(1.0 + GRACE_PERIOD / 1000.0, () -> {
				if (Sys.timeMillis() >= releaseDeadline) {
					releaseSheep();
				}
			});
		}
	}

	void releaseSheep() {
		this.releasedSheep = true;
		System.err.printf("SHEPARD: Releasing: %d sheep\n", waitingSheep.size());

		Set<Endpoint> released = new HashSet<Endpoint>();
		waitingSheep.forEach((e, sheep) -> {
			long when = Math.max(0, GRACE_PERIOD - (Sys.timeMillis() - releaseDeadline));
			sheep.reply(new GrazingGranted(grazingDuration, (int) when));
			released.add(e);
		});
		System.err.printf("SHEPARD: Released: %d sheep\n", waitingSheep.size());
		waitingSheep.keySet().removeAll(released);
	}
}
