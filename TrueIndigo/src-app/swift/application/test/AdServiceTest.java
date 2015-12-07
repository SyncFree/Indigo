package swift.application.test;

import indigo.application.adservice.AdServiceBenchmark;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Semaphore;

import org.junit.Before;
import org.junit.Test;

import swift.exceptions.SwiftException;

public class AdServiceTest {

	static Random random = new Random();

	static String DC_A = "DC_A";
	static String DC_B = "DC_B";
	private boolean started;

	@Before
	public void init2DC() {
		if (!started) {
			TestsUtil.startSequencer("DC_A", "DC_A", 31001, new String[]{"tcp://*:32001/"}, new String[]{"tcp://*:" + 33001 + "/DC_A/", "tcp://*:" + 33002 + "/DC_B/"});
			TestsUtil.startServer("DC_A", "DC_A", 32001, 34001, 35001, 36001, 33001, "tcp://*:31001/", new String[]{"tcp://*:" + 32002 + "/DC_B"});

			TestsUtil.startSequencer("DC_B", "DC_A", 31002, new String[]{"tcp://*:32002/"}, new String[]{"tcp://*:" + 33001 + "/DC_A/", "tcp://*:" + 33002 + "/DC_B/"});
			TestsUtil.startServer("DC_B", "DC_A", 32002, 34002, 35002, 36002, 33002, "tcp://*:31001/", new String[]{"tcp://*:" + 32002 + "/DC_B"});

			started = true;
		}
	}
	@Before
	public void initDB() throws SwiftException, InterruptedException, BrokenBarrierException {
		AdServiceBenchmark service = new AdServiceBenchmark();
		service.initDB(new String[]{"-server", "tcp://*/36001/" + DC_A, "-name", DC_A});
	}

	@Test
	public void test1DC1Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		AdServiceBenchmark service = new AdServiceBenchmark();
		service.doBenchmark(new String[]{"-server", "tcp://*/36001/", "-threads", "1", "-name", DC_A});
	}

	@Test
	public void test1DC10Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		AdServiceBenchmark service = new AdServiceBenchmark();
		service.doBenchmark(new String[]{"-server", "tcp://*/36001/", "-threads", "10", "-name", DC_A});
	}

	@Test
	public void test2DC1Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		AdServiceBenchmark service = new AdServiceBenchmark();
		Semaphore sem = new Semaphore(2);
		sem.acquire(2);
		new Thread(() -> {
			service.doBenchmark(new String[]{"-server", "tcp://*/36001/", "-threads", "1", "-name", DC_A});
			sem.release();
		}).start();
		new Thread(() -> {
			service.doBenchmark(new String[]{"-server", "tcp://*/36002/", "-threads", "1", "-name", DC_B});
			sem.release();
		});
		sem.acquire();
	}

	@Test
	public void test2DC10Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		AdServiceBenchmark service = new AdServiceBenchmark();
		Semaphore sem = new Semaphore(2);
		sem.acquire(2);
		new Thread(() -> {
			service.doBenchmark(new String[]{"-server", "tcp://*/36001/", "-threads", "10", "-name", DC_A});
			sem.release();
		}).start();
		new Thread(() -> {
			service.doBenchmark(new String[]{"-server", "tcp://*/36002/", "-threads", "10", "-name", DC_B});
			sem.release();
		});
		sem.acquire();
	}

}
