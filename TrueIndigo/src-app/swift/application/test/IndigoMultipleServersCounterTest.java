package swift.application.test;

import static org.junit.Assert.assertEquals;
import static swift.application.test.TestsUtil.compareValue;
import static sys.Context.Networking;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import swift.api.CRDTIdentifier;
import swift.crdt.BoundedCounterAsResource;
import swift.exceptions.SwiftException;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.ResourceRequest;
import swift.indigo.remote.IndigoImpossibleExcpetion;
import swift.indigo.remote.RemoteIndigo;

public class IndigoMultipleServersCounterTest {

	static Indigo stub1, stub2;
	static String serversAdresses = "localhost";
	static String table = "COUNTER";
	static char key = '@';

	static Random random = new Random();

	static String DC_A = "DC_A";
	static String DC_B = "DC_B";
	private boolean started;

	@Before
	public void init2DC() {
		if (!started) {
			TestsUtil.startDC1Server("DC_A", 31001, 32001, 33001, 34001, 35001, 36001, new String[]{
					"tcp://*:" + 31001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/"}, new String[]{
					"tcp://*:" + 32001 + "/DC_A/", "tcp://*:" + 32002 + "/DC_B"});
			TestsUtil.startDC1Server("DC_B", 31002, 32002, 33002, 34002, 35002, 36002, new String[]{
					"tcp://*:" + 31001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/"}, new String[]{
					"tcp://*:" + 32001 + "/DC_A", "tcp://*:" + 32002 + "/DC_B"});
			started = true;

			stub1 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/DC_A/"));
			stub2 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36002/DC_B/"));
		}
	}

	@Before
	public void init() throws InterruptedException, SwiftException {
		key++;
		init2DC();
		CRDTIdentifier id = new CRDTIdentifier(table, "" + key);
		initKey(stub1, id, DC_A);
	}

	public static void initKey(Indigo stub, CRDTIdentifier id, String owner) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation(owner, id, 0));
		stub.beginTxn(resources);
		stub.endTxn();
	}

	public void increment(CRDTIdentifier id, int units, Indigo stub, String siteId) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
		x.increment(units, siteId);
		stub.endTxn();
	}

	public boolean decrement(CRDTIdentifier id, int units, Indigo stub, String siteId) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation(siteId, id, units));

		stub.beginTxn(resources);
		BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);

		boolean result = x.decrement(units, siteId);

		stub.endTxn();
		return result;
	}

	public int getValue(CRDTIdentifier id, Indigo stub) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
		stub.endTxn();
		return x.getValue();
	}

	@Test
	public void testTransference() throws SwiftException, InterruptedException {
		CRDTIdentifier id = new CRDTIdentifier(table, key + "");
		increment(id, 10, stub1, DC_A);

		stub2.beginTxn();
		System.out.println("Value " + stub2.get(id, false, BoundedCounterAsResource.class).getValue());
		stub2.endTxn();

		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation("DC_B", id, 5));
		stub2.beginTxn(resources);
		BoundedCounterAsResource crdt = stub2.get(id, false, BoundedCounterAsResource.class);
		crdt.decrement(5, "DC_B");
		stub2.endTxn();
	}

	@Test
	public void testTransferenceMultipelKeys() throws SwiftException, InterruptedException {
		CRDTIdentifier id = new CRDTIdentifier(table, key + "");
		increment(id, 10, stub1, DC_A);

		CRDTIdentifier id2 = new CRDTIdentifier(table, ((char) (key + 1)) + "");
		increment(id2, 10, stub1, DC_A);

		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation("DC_B", id, 5));
		stub2.beginTxn(resources);
		BoundedCounterAsResource crdt = stub2.get(id, false, BoundedCounterAsResource.class);
		crdt.decrement(5, "DC_B");
		stub2.endTxn();

		List<ResourceRequest<?>> resources2 = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation("DC_B", id2, 5));
		stub2.beginTxn(resources);
		BoundedCounterAsResource crdt2 = stub2.get(id, false, BoundedCounterAsResource.class);
		crdt.decrement(5, "DC_B");
		stub2.endTxn();
	}

	@Test
	public void testTransferenceAndDecrement() throws SwiftException, InterruptedException {
		CRDTIdentifier id = new CRDTIdentifier(table, key + "");
		initKey(stub1, id, "DC_A");
		increment(id, 10, stub1, DC_A);

		stub2.beginTxn();
		System.out.println("Value " + stub2.get(id, false, BoundedCounterAsResource.class).getValue());
		stub2.endTxn();

		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation("DC_B", id, 5));
		stub2.beginTxn(resources);
		System.out.println("DC_B: " + stub2.get(id, false, BoundedCounterAsResource.class).decrement(5, DC_B));
		stub2.endTxn();

		stub1.beginTxn(resources);
		System.out.println("DC_A: " + stub1.get(id, false, BoundedCounterAsResource.class).decrement(5, DC_A));
		stub1.endTxn();

	}

	@Test
	public void exhaustRemoteDC1Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		CRDTIdentifier id = new CRDTIdentifier(table, key + "");
		increment(id, 100, stub1, DC_A);

		String dc = DC_B;
		Indigo stub = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36002/" + dc + "/"));

		Semaphore sem = new Semaphore(1);
		sem.acquire();

		new Thread(new Runnable() {
			public void run() {
				try {
					for (;;) {
						if (getValue(id, stub) > 0) {
							decrement(id, 1, stub, dc);
							Thread.sleep(random.nextInt(200));
						} else {
							break;
						}
					}
				} catch (IndigoImpossibleExcpetion e) {
				} catch (SwiftException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					sem.release();
				}
			}
		}).start();
		sem.acquire();
		compareValue(id, 0, stub1);
		compareValue(id, 0, stub2);
	}

	@Test
	public void exhaustRemoteDC10Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		CRDTIdentifier id = new CRDTIdentifier(table, key + "");
		increment(id, 1000, stub1, DC_A);
		int NTHREADS = 10;
		String dc = DC_B;

		Semaphore sem = new Semaphore(NTHREADS);
		sem.acquire(NTHREADS);

		for (int i = 0; i < NTHREADS; i++) {
			final Indigo stub = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36002/" + dc + "/"));
			new Thread(new Runnable() {
				public void run() {
					try {
						for (;;) {
							if (getValue(id, stub) > 0) {
								decrement(id, 1, stub, dc);
								Thread.sleep(random.nextInt(200));
							} else {
								break;
							}
						}
					} catch (IndigoImpossibleExcpetion e) {
					} catch (SwiftException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} finally {
						sem.release();
					}
				}
			}).start();
		}
		sem.acquire(NTHREADS);
		Thread.sleep(10000);
		compareValue(id, 0, stub1);
		compareValue(id, 0, stub2);
	}

	@Test
	public void decrementCycle1Threads2DC() throws SwiftException, InterruptedException, BrokenBarrierException {
		decrementCycleNThreads2DC(200, 1);
	}

	@Test
	public void decrementCycle10Threads2DC() throws SwiftException, InterruptedException, BrokenBarrierException {
		decrementCycleNThreads2DC(1000, 10);
	}

	public void decrementCycleNThreads2DC(int initValue, int nThreadsByDC) throws SwiftException, InterruptedException,
			BrokenBarrierException {
		int count = initValue;
		CRDTIdentifier id = new CRDTIdentifier(table, key + "");
		final AtomicInteger sum1 = new AtomicInteger();
		final AtomicInteger sum2 = new AtomicInteger();
		increment(id, count, stub1, DC_A);
		Semaphore sem = new Semaphore(nThreadsByDC);
		sem.acquire(nThreadsByDC);
		for (int i = 0; i < nThreadsByDC * 2; i++) {
			Indigo stub;
			AtomicInteger sum;
			String dc;
			if (i % 2 == 0) {
				dc = DC_A;
				stub = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/" + dc + "/"));
				sum = sum1;
			} else {
				dc = DC_B;
				stub = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36002/" + dc + "/"));
				sum = sum2;
			}

			new Thread(new Runnable() {
				public void run() {
					try {
						for (;;) {
							if (getValue(id, stub) > 0) {
								if (decrement(id, 1, stub, dc)) {
									sum.incrementAndGet();
								}
								System.out.println(dc + " decrement");
								Thread.sleep(random.nextInt(200));
							} else {
								break;
							}
						}
					} catch (IndigoImpossibleExcpetion e) {
					} catch (SwiftException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} finally {
						sem.release();
					}
				}
			}).start();
		}

		Thread.sleep(2000);
		sem.acquire(nThreadsByDC * 2);
		Thread.sleep(1000);
		compareValue(id, 0, stub1);
		compareValue(id, 0, stub2);
		System.out.println(sum1.get());
		System.out.println(sum2.get());
		assertEquals(count, sum1.get() + sum2.get());
	}
}
