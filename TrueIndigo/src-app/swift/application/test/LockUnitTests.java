package swift.application.test;

import static org.junit.Assert.assertEquals;
import static swift.application.test.TestsUtil.doOp;
import static swift.application.test.TestsUtil.doThreadOp;
import static sys.Context.Networking;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import swift.api.CRDTIdentifier;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.ShareableLock;
import swift.exceptions.SwiftException;
import swift.indigo.Indigo;
import swift.indigo.remote.RemoteIndigo;
import sys.utils.Args;
import sys.utils.Threading;

public class LockUnitTests {

	static Indigo stub1, stub2, stub3;
	static String DC_ID = "DC_A";
	static String serversAdresses = "localhost";
	static String LOCK_TABLE = "LOCK";
	static String table = "REGISTER";
	static char key = 'A';
	private String MASTER_ID;

	@Before
	public void init() throws InterruptedException, SwiftException {
		key++;
		if (stub1 == null || stub2 == null) {
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

			TestsUtil.startDC1Server(DC_ID, MASTER_ID, sequencerPort, serverPort, serverPortForSequencer, dhtPort, pubSubPort, indigoPort, otherSequencers, otherServers);

			stub1 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/" + DC_ID + "/"));
			stub2 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/" + DC_ID + "/"));
			stub3 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/" + DC_ID + "/"));
		}
		initKey();
	}

	public void initKey() throws SwiftException {
		stub1.beginTxn();
		stub1.get(new CRDTIdentifier("LOCK", key + ""), true, EscrowableTokenCRDT.class);
		stub1.endTxn();
	}

	// Gets an exclusive lock and executes the operation
	@Test
	public void simpleSetStringTest() throws Exception {
		CRDTIdentifier idLock = new CRDTIdentifier(LOCK_TABLE, key + "");
		CRDTIdentifier idValue = new CRDTIdentifier(table, key + "");
		doOp(DC_ID, idLock, idValue, "A", ShareableLock.EXCLUSIVE_ALLOW, stub1, 0);
		compareValue(idValue, "A", stub1);
	}

	// Gets an exclusive lock and executes the operation
	@Test
	public void mutatesLockTest() throws Exception {
		CRDTIdentifier idLock = new CRDTIdentifier(LOCK_TABLE, key + "");
		CRDTIdentifier idValue = new CRDTIdentifier(table, key + "");
		doOp(DC_ID, idLock, idValue, "A", ShareableLock.EXCLUSIVE_ALLOW, stub1, 0);
		compareValue(idValue, "A", stub1);
		doOp(DC_ID, idLock, idValue, "B", ShareableLock.FORBID, stub1, 0);
		compareValue(idValue, "B", stub1);
	}

	@Test
	public void multipleSharedAndMutateTest() throws Exception {
		CRDTIdentifier idLock = new CRDTIdentifier(LOCK_TABLE, key + "");
		CRDTIdentifier idValue = new CRDTIdentifier(table, key + "");
		doThreadOp(DC_ID, idLock, idValue, "MODIFIED", ShareableLock.ALLOW, stub1, 10000);
		doThreadOp(DC_ID, idLock, idValue, "MODIFIED", ShareableLock.ALLOW, stub2, 10000);
		doOp(DC_ID, idLock, idValue, "VALUE", ShareableLock.EXCLUSIVE_ALLOW, stub3, 0);
		Threading.sleep(1000);
		compareValue(idValue, "VALUE", stub1);
	}

	@Test
	public void impossibleToGetLockTest() throws Exception {
		CRDTIdentifier idLock = new CRDTIdentifier(LOCK_TABLE, key + "");
		CRDTIdentifier idValue = new CRDTIdentifier(table, key + "");
		doThreadOp(DC_ID, idLock, idValue, "VALUE", ShareableLock.ALLOW, stub1, 5000);

		// Request lock concurrently, while the first is active
		doThreadOp(DC_ID, idLock, idValue, "VALUE", ShareableLock.FORBID, stub1, 0);
		doThreadOp(DC_ID, idLock, idValue, "VALUE", ShareableLock.EXCLUSIVE_ALLOW, stub1, 0);

		Thread.sleep(12000);
	}

	private void compareValue(CRDTIdentifier id, String expected, Indigo stub) throws SwiftException {
		stub.beginTxn();
		LWWRegisterCRDT<String> x = (LWWRegisterCRDT<String>) stub.get(id, false, LWWRegisterCRDT.class);
		assertEquals(expected, x.getValue());
		stub.endTxn();
	}

	@After
	public void close() {
		// Should Stop the nodes.
	}
}
