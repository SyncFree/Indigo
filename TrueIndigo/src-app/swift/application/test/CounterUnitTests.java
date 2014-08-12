package swift.application.test;

import static org.junit.Assert.assertEquals;
import static sys.Context.Networking;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
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

public class CounterUnitTests {

	static Indigo stub11, stub12;
	static String serversAdresses = "localhost";
	static String table = "COUNTER";
	static char key = '@';

	static Random random = new Random();

	static String DC_A = "DC_A";
	static String DC_B = "DC_B";
	private boolean started;

	@Before
	public void init1DC() {
		if (!started) {
			TestsUtil.startDC1Server(DC_A, 31001, 32001, 33001, 34001, 35001, 36001, new String[]{"tcp://*:" + 31001
					+ "/" + DC_A + "/"}, new String[]{});
			started = true;

			stub11 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/" + DC_A + "/"));
			stub12 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/" + DC_A + "/"));
		}
	}

	@Before
	public void init() throws InterruptedException, SwiftException {
		key++;
		init1DC();
		initKey(stub11, DC_A);
	}

	public void initKey(Indigo stub, String siteId) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation(siteId, new CRDTIdentifier(table, "" + key), 0));

		stub.beginTxn(resources);
		stub.endTxn();
		stub.beginTxn();
		stub.get(new CRDTIdentifier(table, "" + key), false, BoundedCounterAsResource.class);
		stub.endTxn();

	}

	@Test
	public void incrementAndRead() throws SwiftException {
		increment("" + key, 10, stub11, DC_A);
		compareValue("" + key, 10, stub11);
	}

	@Test
	public void twoIncrementAndRead() throws SwiftException {
		increment("" + key, 10, stub11, DC_A);
		increment("" + key, 10, stub11, DC_A);
		compareValue("" + key, 20, stub11);
	}
	@Test
	public void decrementAborts() throws SwiftException, InterruptedException {
		increment("" + key, 10, stub11, DC_A);
		assertEquals(true, decrement("" + key, 10, stub11, DC_A));
		Thread.sleep(5000);
		assertEquals(false, decrement("" + key, 10, stub11, DC_A));
	}

	@Test
	public void decrementStillAborts() throws SwiftException {
		increment("" + key, 9, stub11, DC_A);
		assertEquals(false, decrement("" + key, 10, stub11, DC_A));
	}

	@Test
	public void decrementSucceeds() throws SwiftException {
		increment("" + key, 10, stub11, DC_A);
		assertEquals(true, decrement("" + key, 10, stub11, DC_A));
	}

	@Test
	public void decrementCycle1Threads1DC() throws SwiftException, InterruptedException, BrokenBarrierException {
		decrementCycleNThreads1DC(100, 1);
	}

	@Test
	public void decrementCycle2Threads1DC() throws SwiftException, InterruptedException, BrokenBarrierException {
		decrementCycleNThreads1DC(200, 2);
	}

	@Test
	public void decrementCycle10Threads1DC() throws SwiftException, InterruptedException, BrokenBarrierException {
		decrementCycleNThreads1DC(1000, 10);
	}

	@Test
	public void decrementCycle100Threads1DC() throws SwiftException, InterruptedException, BrokenBarrierException {
		decrementCycleNThreads1DC(10000, 100);
	}

	public void decrementCycleNThreads1DC(int initValue, int nThreads) throws SwiftException, InterruptedException,
			BrokenBarrierException {
		int count = initValue;
		final AtomicInteger sum = new AtomicInteger();
		increment("" + key, count, stub11, DC_A);
		Semaphore sem = new Semaphore(nThreads);
		sem.acquire(nThreads);
		for (int i = 0; i < nThreads; i++) {
			final Indigo stub = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/" + DC_A + "/"));

			new Thread(new Runnable() {
				public void run() {
					int i = 0;
					try {
						for (;; i++) {
							if (getValue("" + key, stub) > 0) {
								decrement("" + key, 1, stub, DC_A);
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
						sum.addAndGet(i);
						sem.release();
					}
				}
			}).start();
		}

		Thread.sleep(2000);
		sem.acquire(nThreads);
		Thread.sleep(1000);
		compareValue("" + key, 0, stub11);
		assertEquals(count, sum.get());
	}

	@Test
	public void waitAndSucceed() throws SwiftException, InterruptedException {
		increment("" + key, 10, stub11, DC_A);

		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					decrement("" + key, 10, stub11, DC_A);
					System.out.println("Decrement and sleep");
					Thread.sleep(500);
					increment("" + key, 10, stub11, DC_A);
					System.out.println("First thread increments and finish");
				} catch (SwiftException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		}).start();

		Thread.sleep(500);

		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					decrement("" + key, 8, stub12, DC_A);
					System.out.println("Second thread succeeded");
				} catch (SwiftException e) {
					e.printStackTrace();
				}

			}
		}).start();

		Thread.sleep(2000);
	}

	@Test
	public void simpleTestTransfer() throws SwiftException {

	}

	public void increment(String key, int units, Indigo stub, String siteId) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(new CRDTIdentifier(table, key), false, BoundedCounterAsResource.class);
		x.increment(units, siteId);
		stub.endTxn();

	}

	public boolean decrement(String key, int units, Indigo stub, String siteId) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation(siteId, new CRDTIdentifier(table, key), units));

		stub.beginTxn(resources);
		BoundedCounterAsResource x = stub.get(new CRDTIdentifier(table, key), false, BoundedCounterAsResource.class);

		boolean result = x.decrement(units, siteId);

		stub.endTxn();
		return result;
	}

	public void compareValue(String key, int expected, Indigo stub) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(new CRDTIdentifier(table, key), false, BoundedCounterAsResource.class);
		assertEquals((Integer) expected, x.getValue());
		stub.endTxn();
	}

	public int getValue(String key, Indigo stub) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(new CRDTIdentifier(table, key), false, BoundedCounterAsResource.class);
		stub.endTxn();
		return x.getValue();
	}

	@After
	public void close() {
		// Should Stop the nodes.
	}

}
