package swift.application.test;

import static org.junit.Assert.assertEquals;
import static swift.application.test.TestsUtil.doThreadOp;
import static sys.Context.Networking;

import java.util.LinkedList;
import java.util.List;

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
import sys.utils.Threading;

public class IndigoTwoServersLockTest {

	private static final String LOCK_TABLE = "LOCK";
	static boolean started;
	private static Indigo stub1;
	private static Indigo stub2;

	static String table = "Register";
	static char key = '@';

	@Before
	public void init2DC() {
		if (!started) {
			TestsUtil.startSequencer("DC_A", "DC_A", 31001, new String[]{"tcp://*:32001/"}, new String[]{"tcp://*:" + 33001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/"});
			TestsUtil.startServer("DC_A", "DC_A", 32001, 34001, 35001, 36001, 33001, "tcp://*:31001/", new String[]{"tcp://*:" + 32002 + "/DC_B"});

			TestsUtil.startSequencer("DC_B", "DC_A", 31002, new String[]{"tcp://*:32002/"}, new String[]{"tcp://*:" + 33001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/"});
			TestsUtil.startServer("DC_B", "DC_A", 32002, 34002, 35002, 36002, 33002, "tcp://*:31001/", new String[]{"tcp://*:" + 32002 + "/DC_B"});

			started = true;

			stub1 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/DC_A/"));
			stub2 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36002/DC_B/"));
		}
	}

	@Before
	public void updateTestKey() {
		key++;
	}

	@SuppressWarnings("unchecked")
	public void initKey(Indigo stub) throws SwiftException {
		stub.beginTxn();
		CRDTIdentifier id = new CRDTIdentifier(LOCK_TABLE, key + "");
		EscrowableTokenCRDT lock = stub.get(id, true, EscrowableTokenCRDT.class);
		lock.apply("DC_A", new LockReservation("DC_A", id, ShareableLock.ALLOW));
		stub.get(new CRDTIdentifier(table, key + ""), true, LWWRegisterCRDT.class);
		stub.endTxn();
	}

	@Test
	public void testReplication() throws SwiftException, InterruptedException {
		initKey(stub1);
		CRDTIdentifier id = new CRDTIdentifier(LOCK_TABLE, key + "");
		stub1.beginTxn();
		ShareableLock typeDC1 = stub1.get(id, false, EscrowableTokenCRDT.class).getValue();
		stub1.endTxn();

		Thread.sleep(1000);
		stub2.beginTxn();
		ShareableLock typeDC2 = stub2.get(id, false, EscrowableTokenCRDT.class).getValue();
		stub2.endTxn();

		assertEquals(typeDC1, typeDC2);
	}

	@Test
	public void testLocalChangeType() throws SwiftException, InterruptedException {
		initKey(stub1);
		CRDTIdentifier idLock = new CRDTIdentifier(LOCK_TABLE, "" + key);
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new LockReservation("DC_A", idLock, ShareableLock.FORBID));
		stub1.beginTxn(resources);
		stub1.get(idLock, false, EscrowableTokenCRDT.class).getValue();
		stub1.endTxn();
	}

	@Test
	public void testTransferenceSameType() throws SwiftException, InterruptedException {
		initKey(stub1);
		CRDTIdentifier idLock = new CRDTIdentifier(LOCK_TABLE, "" + key);
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new LockReservation("DC_B", idLock, ShareableLock.ALLOW));
		stub2.beginTxn(resources);
		ShareableLock value = stub2.get(idLock, false, EscrowableTokenCRDT.class).getValue();
		stub2.endTxn();
		assertEquals(ShareableLock.ALLOW, value);
	}

	@Test
	public void testTransferenceAndAcquireSameType() throws SwiftException, InterruptedException {
		initKey(stub1);
		CRDTIdentifier idLock = new CRDTIdentifier(LOCK_TABLE, "" + key);
		CRDTIdentifier idValue = new CRDTIdentifier(table, "" + key);
		doThreadOp("DC_B", idLock, idValue, "MODIFIED", ShareableLock.FORBID, stub2, 3000);
		Threading.sleep(1000);
		doThreadOp("DC_A", idLock, idValue, "MODIFIED", ShareableLock.FORBID, stub1, 1000);
		Threading.sleep(5000);
		stub1.beginTxn();
		ShareableLock newType = stub1.get(idLock, false, EscrowableTokenCRDT.class).getValue();
		stub1.endTxn();
		assertEquals(ShareableLock.FORBID, newType);
	}

	@Test
	public void testTransferenceAndAcquireDifferentType() throws SwiftException, InterruptedException {
		initKey(stub1);
		CRDTIdentifier idLock = new CRDTIdentifier(LOCK_TABLE, "" + key);
		CRDTIdentifier idValue = new CRDTIdentifier(table, "" + key);
		doThreadOp("DC_B", idLock, idValue, "MODIFIED", ShareableLock.FORBID, stub2, 3000);
		Threading.sleep(1000);
		doThreadOp("DC_A", idLock, idValue, "MODIFIED FINAL", ShareableLock.EXCLUSIVE_ALLOW, stub1, 0);
		Threading.sleep(2000);
		stub1.beginTxn();
		@SuppressWarnings("unchecked")
		String value = (String) stub1.get(idValue, false, LWWRegisterCRDT.class).getValue();
		stub1.endTxn();
		assertEquals("MODIFIED FINAL", value);
	}

}
