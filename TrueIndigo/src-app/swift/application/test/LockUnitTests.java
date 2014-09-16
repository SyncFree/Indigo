package swift.application.test;

import static org.junit.Assert.assertEquals;
import static sys.Context.Networking;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import swift.api.CRDTIdentifier;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.ShareableLock;
import swift.exceptions.SwiftException;
import swift.indigo.Indigo;
import swift.indigo.LockReservation;
import swift.indigo.ResourceRequest;
import swift.indigo.remote.RemoteIndigo;
import sys.utils.Args;

public class LockUnitTests {

	static Indigo stub;
	static String DC_ID = "DC_A";
	static String serversAdresses = "localhost";
	static String table = "REGISTER";
	static char key = 'A';
	private String MASTER_ID;

	@Before
	public void init() throws InterruptedException, SwiftException {
		key++;
		if (stub == null) {
			System.out.printf("Start DataCenter: %s", DC_ID);
			DC_ID = Args.valueOf("-siteId", "X");
			MASTER_ID = Args.valueOf("-master", DC_ID);
			int sequencerPort = Args.valueOf("-seqPort", 31001);
			int serverPort = Args.valueOf("-srvPort", 32001);
			int serverPortForSequencer = Args.valueOf("-sFs", 33001);
			int dhtPort = Args.valueOf("-dhtPort", 34001);
			int pubSubPort = Args.valueOf("-pubSubPort", 35001);
			int indigoPort = Args.valueOf("-indigoPort", 36001);
			String[] otherSequencers = Args.valueOf("-sequencers", new String[]{"tcp://*/31001/" + DC_ID + "/"});
			String[] otherServers = Args.valueOf("-servers", new String[]{"tcp://*/32001/" + DC_ID + "/"});

			TestsUtil.startDC1Server(DC_ID, MASTER_ID, sequencerPort, serverPort, serverPortForSequencer, dhtPort,
					pubSubPort, indigoPort, otherSequencers, otherServers);

			stub = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/" + DC_ID + "/"));
		}
		initKey();
	}

	public void initKey() throws SwiftException {
		stub.beginTxn();
		EscrowableTokenCRDT obj = stub.get(new CRDTIdentifier("LOCK", "A"), true, EscrowableTokenCRDT.class);
		stub.endTxn();
	}
	public static void doOp(String siteId, String key, String value, ShareableLock lock, long sleepBeforeCommit)
			throws Exception {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		LockReservation request = new LockReservation(siteId, new CRDTIdentifier("LOCK", "A"), lock);
		resources.add(request);

		stub.beginTxn(resources);
		LWWRegisterCRDT<String> register = (LWWRegisterCRDT<String>) stub.get(new CRDTIdentifier(table, key), true,
				LWWRegisterCRDT.class);
		register.set(value);

		Thread.sleep(sleepBeforeCommit);

		stub.endTxn();

	}

	public static void doThreadOp(final String siteId, final String key, final String value, final ShareableLock lock,
			final long sleepBeforeCommit) {
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					doOp(siteId, key, value, lock, sleepBeforeCommit);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	// Gets an exclusive lock and executes the operation
	@Test
	public void simpleSetStringTest() throws Exception {
		doOp(DC_ID, "" + key, "A", ShareableLock.EXCLUSIVE_ALLOW, 0);
		getValue("" + key, "A");
	}

	// Gets an exclusive lock and executes the operation
	@Test
	public void mutatesLockTest() throws Exception {
		doOp(DC_ID, "" + key, "A", ShareableLock.EXCLUSIVE_ALLOW, 0);
		getValue("" + key, "A");
		doOp(DC_ID, "" + key, "B", ShareableLock.FORBID, 0);
		getValue("" + key, "B");
	}

	@Test
	public void impossibleToGetLockTest() throws Exception {
		doThreadOp(DC_ID, "" + key, "VALUE", ShareableLock.ALLOW, 5000);

		// Request lock concurrently, while the first is active
		doThreadOp(DC_ID, "" + key, "VALUE", ShareableLock.FORBID, 0);
		doThreadOp(DC_ID, "" + key, "VALUE", ShareableLock.EXCLUSIVE_ALLOW, 0);

		Thread.sleep(12000);
	}

	public void getValue(String key, String expected) throws SwiftException {
		stub.beginTxn();
		LWWRegisterCRDT<String> x = (LWWRegisterCRDT<String>) stub.get(new CRDTIdentifier(table, key), false,
				LWWRegisterCRDT.class);

		assertEquals(expected, x.getValue());

		stub.endTxn();

	}

	@After
	public void close() {
		// Should Stop the nodes.
	}
}
