package swift.application.test;

import static org.junit.Assert.assertEquals;
import static sys.Context.Networking;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import swift.api.CRDTIdentifier;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.ShareableLock;
import swift.exceptions.SwiftException;
import swift.indigo.Indigo;
import swift.indigo.LockReservation;
import swift.indigo.ResourceRequest;
import swift.indigo.remote.RemoteIndigo;

public class IndigoMultipleServersLockTest {

	static boolean started;
	private static Indigo stub1;
	private static Indigo stub2;

	static String table = "LOCK";
	static char key = '@';

	@Before
	public void init2DC() {
		if (!started) {
			TestsUtil.startDC1Server("DC_A", 31001, 32001, 33001, 34001, 35001, 36001, new String[]{
					"tcp://*:" + 31001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/"}, new String[]{"tcp://*:" + 32002
					+ "/DC_B"});
			TestsUtil.startDC1Server("DC_B", 31002, 32002, 33002, 34002, 35002, 36002, new String[]{
					"tcp://*:" + 31001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/"}, new String[]{"tcp://*:" + 32001
					+ "/DC_A"});
			started = true;

			stub1 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/DC_A/"));
			stub2 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36002/DC_B/"));
		}
	}

	@Before
	public void updateTestKey() {
		key++;
	}

	public static void initKey(Indigo stub, CRDTIdentifier id, String owner) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new LockReservation(owner, id, ShareableLock.ALLOW));
		stub.beginTxn(resources);
		stub.endTxn();
		stub.beginTxn();
		stub.endTxn();
	}

	@Test
	public void testReplication() throws SwiftException, InterruptedException {
		CRDTIdentifier id = new CRDTIdentifier(table, "" + key);
		initKey(stub1, id, "DC_A");

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
		CRDTIdentifier id = new CRDTIdentifier(table, "" + key);
		initKey(stub1, id, "DC_A");
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new LockReservation("DC_A", id, ShareableLock.FORBID));
		stub1.beginTxn(resources);
		stub1.get(id, false, EscrowableTokenCRDT.class).getValue();
		stub1.endTxn();
	}

	@Test
	public void testTransferenceSameType() throws SwiftException, InterruptedException {
		CRDTIdentifier id = new CRDTIdentifier(table, "" + key);
		initKey(stub1, id, "DC_A");
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new LockReservation("DC_B", id, ShareableLock.ALLOW));
		stub2.beginTxn(resources);
		stub2.get(id, false, EscrowableTokenCRDT.class).getValue();
		stub2.endTxn();
	}

	@Test
	public void testTransferenceDifferentType() throws SwiftException, InterruptedException {
		CRDTIdentifier id = new CRDTIdentifier(table, "" + key);
		initKey(stub1, id, "DC_A");
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new LockReservation("DC_B", id, ShareableLock.FORBID));
		stub2.beginTxn(resources);
		ShareableLock newType = stub2.get(id, false, EscrowableTokenCRDT.class).getValue();
		System.out.println(newType);
		stub2.endTxn();
		System.out.println("FINISHED COMMIT");
		assertEquals(ShareableLock.FORBID, newType);
	}

	@Test
	public void testTransferenceTwoTypes() throws SwiftException, InterruptedException {
		CRDTIdentifier id = new CRDTIdentifier(table, "" + key);
		initKey(stub1, id, "DC_A");

		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new LockReservation("DC_B", id, ShareableLock.ALLOW));
		stub2.beginTxn(resources);
		ShareableLock firstType = stub2.get(id, false, EscrowableTokenCRDT.class).getValue();
		System.out.println(firstType);
		stub2.endTxn();

		resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new LockReservation("DC_B", id, ShareableLock.FORBID));
		stub2.beginTxn(resources);
		ShareableLock secondType = stub2.get(id, false, EscrowableTokenCRDT.class).getValue();
		System.out.println(secondType);
		stub2.endTxn();

		assertEquals(ShareableLock.ALLOW, firstType);
		assertEquals(ShareableLock.FORBID, secondType);
	}

}
