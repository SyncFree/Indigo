package swift.application.test;

import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

import org.junit.Test;

import swift.api.CRDTIdentifier;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import swift.crdt.ShareableLock;
import swift.indigo.CounterReservation;
import swift.indigo.IndigoOperation;
import swift.indigo.LockReservation;
import swift.indigo.ResourceRequest;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.ReleaseResourcesRequest;
import swift.indigo.proto.TransferResourcesRequest;

public class TestMessageOrdering {

	@Test
	public void testDifferentMessagesOrdering() {
		IncrementalTimestampGenerator tsGenerator = new IncrementalTimestampGenerator("SITE");
		PriorityQueue<IndigoOperation> queue = new PriorityQueue<IndigoOperation>();
		Queue<IndigoOperation> list = new LinkedList<IndigoOperation>();
		Collection<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();

		Timestamp msg1TS = tsGenerator.generateNew();
		AcquireResourcesRequest msg1 = new AcquireResourcesRequest("ID_A", msg1TS, resources);
		ReleaseResourcesRequest msg2 = new ReleaseResourcesRequest(msg1TS);
		TransferResourcesRequest msg3 = new TransferResourcesRequest("ID_A", 1, msg1);

		queue.add(msg1);
		queue.add(msg2);
		queue.add(msg3);

		list.add(msg2);
		list.add(msg3);
		list.add(msg1);

		System.out.println(list);

		assertEquals(list.remove(), queue.remove());
		assertEquals(list.remove(), queue.remove());
		assertEquals(list.remove(), queue.remove());

	}

	@Test
	public void testEqualAcquireMsgOrdering() {
		IncrementalTimestampGenerator tsGenerator = new IncrementalTimestampGenerator("SITE");
		Collection<ResourceRequest<?>> resources1 = new LinkedList<ResourceRequest<?>>();
		Collection<ResourceRequest<?>> resources2 = new LinkedList<ResourceRequest<?>>();

		resources1.add(new CounterReservation(new CRDTIdentifier(), 10));
		resources2.add(new CounterReservation(new CRDTIdentifier(), 10));

		Timestamp msg1TS = tsGenerator.generateNew();
		Timestamp msg2TS = tsGenerator.generateNew();
		AcquireResourcesRequest msg1 = new AcquireResourcesRequest("ID_A", msg1TS, resources1);
		AcquireResourcesRequest msg2 = new AcquireResourcesRequest("ID_A", msg2TS, resources2);

		assertEquals(-1, msg1.compareTo(msg2));
	}

	@Test
	public void testDifferentAcquireMsgOrdering() {
		IncrementalTimestampGenerator tsGenerator = new IncrementalTimestampGenerator("SITE");
		Collection<ResourceRequest<?>> resources1 = new LinkedList<ResourceRequest<?>>();
		Collection<ResourceRequest<?>> resources2 = new LinkedList<ResourceRequest<?>>();

		resources1.add(new CounterReservation(new CRDTIdentifier(), 5));
		resources2.add(new CounterReservation(new CRDTIdentifier(), 10));

		Timestamp msg1TS = tsGenerator.generateNew();
		Timestamp msg2TS = tsGenerator.generateNew();
		AcquireResourcesRequest msg1 = new AcquireResourcesRequest("ID_A", msg1TS, resources1);
		AcquireResourcesRequest msg2 = new AcquireResourcesRequest("ID_A", msg2TS, resources2);

		assertEquals(true, msg1.compareTo(msg2) < 0);
	}

	@Test
	public void testLockCounterOrdering() {
		IncrementalTimestampGenerator tsGenerator = new IncrementalTimestampGenerator("SITE");
		Collection<ResourceRequest<?>> resources1 = new LinkedList<ResourceRequest<?>>();
		Collection<ResourceRequest<?>> resources2 = new LinkedList<ResourceRequest<?>>();

		resources1.add(new CounterReservation(new CRDTIdentifier(), 5));
		resources2.add(new LockReservation("R2", new CRDTIdentifier(), ShareableLock.ALLOW));

		Timestamp msg1TS = tsGenerator.generateNew();
		AcquireResourcesRequest msg1 = new AcquireResourcesRequest("ID_A", msg1TS, resources1);
		AcquireResourcesRequest msg2 = new AcquireResourcesRequest("ID_A", msg1TS, resources2);

		assertEquals(true, msg2.compareTo(msg1) < 0);
	}

	@Test
	public void testCounterCounterOrdering() {
		IncrementalTimestampGenerator tsGenerator = new IncrementalTimestampGenerator("SITE");
		Collection<ResourceRequest<?>> resources1 = new LinkedList<ResourceRequest<?>>();
		Collection<ResourceRequest<?>> resources2 = new LinkedList<ResourceRequest<?>>();

		resources1.add(new CounterReservation(new CRDTIdentifier(), 10));
		resources2.add(new CounterReservation(new CRDTIdentifier(), 5));

		Timestamp msg1TS = tsGenerator.generateNew();
		AcquireResourcesRequest msg1 = new AcquireResourcesRequest("ID_A", msg1TS, resources1);
		AcquireResourcesRequest msg2 = new AcquireResourcesRequest("ID_A", msg1TS, resources2);

		assertEquals(true, msg2.compareTo(msg1) < 0);
	}

	@Test
	public void testLockLockOrdering() {
		IncrementalTimestampGenerator tsGenerator = new IncrementalTimestampGenerator("SITE");
		Collection<ResourceRequest<?>> resources1 = new LinkedList<ResourceRequest<?>>();
		Collection<ResourceRequest<?>> resources2 = new LinkedList<ResourceRequest<?>>();
		Collection<ResourceRequest<?>> resources3 = new LinkedList<ResourceRequest<?>>();
		Collection<ResourceRequest<?>> resources4 = new LinkedList<ResourceRequest<?>>();
		PriorityQueue<IndigoOperation> queue = new PriorityQueue<IndigoOperation>();
		Queue<IndigoOperation> list = new LinkedList<IndigoOperation>();

		resources1.add(new LockReservation("R1", new CRDTIdentifier(), ShareableLock.ALLOW));
		resources2.add(new LockReservation("R2", new CRDTIdentifier(), ShareableLock.ALLOW));
		resources3.add(new LockReservation("R3", new CRDTIdentifier(), ShareableLock.FORBID));
		resources4.add(new LockReservation("R4", new CRDTIdentifier(), ShareableLock.EXCLUSIVE_ALLOW));

		Timestamp msg1TS = tsGenerator.generateNew();
		AcquireResourcesRequest msg1 = new AcquireResourcesRequest("ID_A", msg1TS, resources1);
		AcquireResourcesRequest msg2 = new AcquireResourcesRequest("ID_A", msg1TS, resources2);
		AcquireResourcesRequest msg3 = new AcquireResourcesRequest("ID_A", msg1TS, resources3);
		AcquireResourcesRequest msg4 = new AcquireResourcesRequest("ID_A", msg1TS, resources4);

		queue.add(msg1);
		queue.add(msg2);
		queue.add(msg3);
		queue.add(msg4);

		list.add(msg4);
		list.add(msg1);
		list.add(msg2);
		list.add(msg3);

		// ORDER: EXCLUSIVE, ALLOW, FORBID
		assertEquals(list.remove(), queue.remove());
		assertEquals(list.remove(), queue.remove());
		assertEquals(list.remove(), queue.remove());
		assertEquals(list.remove(), queue.remove());

	}

}
