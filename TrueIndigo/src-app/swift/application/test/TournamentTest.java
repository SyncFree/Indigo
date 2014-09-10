package swift.application.test;

import indigo.application.tournament.TournamentServiceBenchmark;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Semaphore;

import org.junit.Before;
import org.junit.Test;

import swift.exceptions.SwiftException;

public class TournamentTest {

	static Random random = new Random();

	static String DC_A = "DC_A";
	static String DC_B = "DC_B";
	private boolean started;

	@Before
	public void init2DC() {
		if (!started) {
			TestsUtil.startDC1Server("DC_A", "DC_A", 31001, 32001, 33001, 34001, 35001, 36001, new String[]{
					"tcp://*:" + 31001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/"}, new String[]{"tcp://*:" + 32002
					+ "/DC_B"});
			TestsUtil.startDC1Server("DC_B", "DC_A", 31002, 32002, 33002, 34002, 35002, 36002, new String[]{
					"tcp://*:" + 31001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/"}, new String[]{"tcp://*:" + 32001
					+ "/DC_A"});
			started = true;
		}
	}
	@Before
	public void initDB() throws SwiftException, InterruptedException, BrokenBarrierException {
		TournamentServiceBenchmark service = new TournamentServiceBenchmark();
		service.initDB(new String[]{"-server", "tcp://*/36001/" + DC_A, "-name", DC_A});
	}

	@Test
	public void test1DC1Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		TournamentServiceBenchmark service = new TournamentServiceBenchmark();
		service.doBenchmark(new String[]{"-server", "tcp://*/36001/", "-threads", "1", "-name", DC_A});
	}

	@Test
	public void test1DC10Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		TournamentServiceBenchmark service = new TournamentServiceBenchmark();
		service.doBenchmark(new String[]{"-server", "tcp://*/36001/", "-threads", "10", "-name", DC_A});
	}

	@Test
	public void test2DC1Thread() throws SwiftException, InterruptedException, BrokenBarrierException {
		TournamentServiceBenchmark service = new TournamentServiceBenchmark();
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
		TournamentServiceBenchmark service = new TournamentServiceBenchmark();
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
