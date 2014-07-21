package swift.application.test;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import swift.api.CRDTIdentifier;
import swift.crdt.IntegerCRDT;
import swift.crdt.ShareableLock;
import swift.indigo.Indigo;
import swift.indigo.IndigoAppServer;
import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.LockReservation;
import sys.shepard.Shepard;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Threading;

public class IndigoServerTest implements Runnable {

	Indigo stub;
	String hostname = IP.localHostname();

	IndigoServerTest() {
		this.stub = IndigoAppServer.getIndigo();
	}

	public void doApp1() throws Exception {
		final LockReservation[] locks = new LockReservation[]{new LockReservation("xxx", ShareableLock.FORBID), new LockReservation("yyy", ShareableLock.ALLOW)};

		System.err.println("STARTING------>" + hostname + "\n\n\n");

		stub.beginTxn(locks);
		IntegerCRDT x = stub.get(new CRDTIdentifier("/foo", "test"), true, IntegerCRDT.class);
		x.add(66);
		stub.endTxn();

		stub.beginTxn(locks);
		x = stub.get(new CRDTIdentifier("/foo", "test"), true, IntegerCRDT.class);
		x.add(66);
		stub.endTxn();

		stub.beginTxn(locks);
		IntegerCRDT y = stub.get(new CRDTIdentifier("/foo", "test2"), true, IntegerCRDT.class);
		y.add(666);
		stub.endTxn();

		stub.beginTxn(locks);
		IntegerCRDT z = stub.get(new CRDTIdentifier("/foo", "test"), true, IntegerCRDT.class);
		System.out.println(hostname + "  " + InetAddress.getLocalHost().getHostName() + "    " + z.getValue());
		stub.endTxn();

		System.err.println("ENDING------>" + hostname + "\n\n\n");
	}

	public void doApp2() throws Exception {
		final LockReservation[] locks = new LockReservation[]{new LockReservation("xxx", ShareableLock.EXCLUSIVE_ALLOW), new LockReservation("yyy", ShareableLock.EXCLUSIVE_ALLOW)};

		System.err.println("STARTING------>" + hostname + "\n\n\n");

		stub.beginTxn(locks);
		IntegerCRDT x = stub.get(new CRDTIdentifier("/foo", "test"), true, IntegerCRDT.class);
		x.add(66);
		stub.endTxn();

		System.err.println("ENDING------>" + hostname + "\n\n\n");
	}

	public void run() {
		try {
			Threading.sleep(5000);

			doApp1();

			Threading.sleep(20000);
			doApp2();

		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	// void initDB() {
	// final Lock[] locks = new Lock[] { new Lock("xxx", LockType.FORBID), new
	// Lock("yyy", LockType.ALLOW) };
	//
	// System.err.println(hostname + "--->initLocks begin...");
	// stub.beginTxn(locks);
	// stub.endTxn();
	// System.err.println(hostname + "--->initLocks done...");
	// }

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.err.println("Nothing to do...[Missing program args...]");
			System.exit(0);
		}

		IndigoSequencerAndResourceManager.main(new String[]{"-name", "X0"});
		IndigoAppServer.main(args);

		// if (Args.contains(args, "-init")) {
		// new IndigoServerTest().initDB();
		// System.exit(0);
		// }

		if (Args.contains(args, "-run")) {
			int threads = Args.valueOf(args, "-threads", 1);

			String shepard = Args.valueOf(args, "-shepard", "");

			if (!shepard.isEmpty())
				Shepard.sheepJoinHerd(shepard);

			final ExecutorService executor = Executors.newFixedThreadPool(threads);
			for (int i = 0; i < threads; i++)
				executor.execute(new IndigoServerTest());

			executor.awaitTermination(60, TimeUnit.SECONDS);

			System.exit(0);
		}

		System.err.println("Great. Nothing to do...");
	}
}
