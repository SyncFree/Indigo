package indigo.application.benchmark;

import static sys.Context.Networking;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Semaphore;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;

import swift.api.CRDTIdentifier;
import swift.application.test.TestsUtil;
import swift.crdt.BoundedCounterAsResource;
import swift.dc.Defaults;
import swift.exceptions.SwiftException;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.ResourceRequest;
import swift.indigo.remote.IndigoImpossibleException;
import swift.indigo.remote.RemoteIndigo;
import swift.proto.CurrentClockRequest;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.shepard.PatientShepard;
import sys.utils.Args;
import sys.utils.Profiler;

public class MicroBenchmark {
	static String table;
	static String DC_ADDRESS = "tcp://*/36001/";
	static String DC_ID = "X";
	static IntegerDistribution distribution;
	static Random uniformRandom = new Random();
	private static int nKeys;
	private static String resultsLogName = "MicroBenchmark";
	private static Profiler profiler;
	private static String MASTER_ID;

	public static void decrementCycleNThreads(int nThreadsByDC, int maxThinkTime, final int nWrites, int maxNReads) throws SwiftException, InterruptedException, BrokenBarrierException {
		System.out.printf("Start decrementCycleNThreads microbenchmark: %d threads %d sleep time at site %s %s.\n", nThreadsByDC, maxThinkTime, DC_ID, DC_ADDRESS);
		final String dc_id;
		if (Args.contains("-weak")) {
			dc_id = "GLOBAL";
		} else {
			dc_id = DC_ID;
		}
		Semaphore sem = new Semaphore(nThreadsByDC);
		sem.acquire(nThreadsByDC);
		for (int i = 0; i < nThreadsByDC; i++) {
			Indigo stub = RemoteIndigo.getInstance(Networking.resolve(DC_ADDRESS + "/"));
			Service fakeStub = Args.contains("-fakeCS") ? Networking.stub() : null;
			Endpoint surrogate = Networking.resolve(Args.valueOf("-fakeCS", ""), Defaults.SERVER_URL);

			Thread t = new Thread(new Runnable() {
				public void run() {
					boolean result = true;
					Set<CRDTIdentifier> excludedIds = new HashSet<>();
					try {
						while (result) {
							// 50:50 workload
							if (nWrites >= 1) {
								List<CRDTIdentifier> ids = getNKeys(nWrites, excludedIds);
								try {
									result = decrementNValues(ids, 1, stub, dc_id, fakeStub, surrogate);
								} catch (IndigoImpossibleException e) {
									excludedIds.addAll(e.getResources());
									if (excludedIds.size() == nKeys)
										result = false;
								}
								Thread.sleep(maxThinkTime - uniformRandom.nextInt(maxThinkTime / 2));
							}
							if (maxNReads > 0) {
								int nReads = nKeys > 1 ? uniformRandom.nextInt(maxNReads) + 1 : 1;
								List<CRDTIdentifier> ids = getNKeys(nReads, excludedIds);
								readNValues(ids, 1, stub, dc_id, fakeStub, surrogate);
								Thread.sleep(maxThinkTime - uniformRandom.nextInt(maxThinkTime / 2));
							}

						}
					} catch (SwiftException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					} finally {
						sem.release();
					}
				}

				private List<CRDTIdentifier> getNKeys(int nKeys, Set<CRDTIdentifier> excludedIds) {
					List<CRDTIdentifier> ids = new LinkedList<>();
					for (int i = 0; i < nKeys; i++) {
						String key = distribution.sample() + "";
						CRDTIdentifier id = new CRDTIdentifier(table + "", key);
						if (excludedIds.contains(id)) {
							i--;
							continue;
						} else {
							ids.add(id);
						}
					}
					return ids;
				}
			});
			t.start();
		}

		Thread.sleep(2000);
		sem.acquire(nThreadsByDC);
		System.out.println("All clients stopped");
		System.exit(0);
	}
	public static boolean decrementNValues(List<CRDTIdentifier> ids, int units, Indigo stub, String siteId, Service fakeStub, Endpoint surrogate) throws SwiftException, IndigoImpossibleException {
		long opId = profiler.startOp(resultsLogName, "DEC");
		int counterValue = -999;
		int availableSite = -999;
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		for (CRDTIdentifier id : ids) {
			resources.add(new CounterReservation(siteId, id, units));
		}
		boolean result = false;
		try {
			stub.beginTxn(resources);
			for (CRDTIdentifier id : ids) {
				BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
				if ((x.getValue()) > 0) {
					result = x.decrement(units, siteId);
					if (result == false) {
						System.out.println("FAILED");
					}
					counterValue = x.getValue();
					availableSite = x.getSiteResource(siteId);
				}
			}
		} finally {
			stub.endTxn();
			if (fakeStub != null)
				fakeStub.request(surrogate, new CurrentClockRequest());
			profiler.endOp(resultsLogName, opId, counterValue + "", result + "", availableSite + "", ids.size() + "", 0 + "");
		}
		// System.out.println(DC_ID + " " + counterValue + " " + result + " " +
		// availableSite + " ");
		return result;
	}

	public static void readNValues(List<CRDTIdentifier> ids, int units, Indigo stub, String siteId, Service fakeStub, Endpoint surrogate) throws SwiftException {
		long opId = profiler.startOp(resultsLogName, "READ");
		int counterValue = -999;
		int availableSite = -999;
		try {
			stub.beginTxn();
			for (CRDTIdentifier id : ids) {
				stub.get(id, false, BoundedCounterAsResource.class);
			}
		} catch (IndigoImpossibleException e) {
		} finally {
			stub.endTxn();
			if (fakeStub != null)
				fakeStub.request(surrogate, new CurrentClockRequest());
		}
		// System.out.println(DC_ID + " " + counterValue + " " + result + " " +
		// availableSite + " ");
		profiler.endOp(resultsLogName, opId, counterValue + "", true + "", availableSite + "", 0 + "", ids.size() + "");
	}

	public void setNormalDistribution(int sampleSize) {
		distribution = new UniformIntegerDistribution(0, sampleSize);
	}

	public void setZipfDistribution(int sampleSize, int exponent) {
		distribution = new ZipfDistribution(sampleSize, exponent);
	}

	public static void initCounters(int nKeys, String table, int initValue, int valueVariation, String ownerId, Indigo stub) throws SwiftException {
		System.out.println("Initializing Counters");
		int transactionSize = 100;
		LinkedList<ResourceRequest<?>> counterRequests = new LinkedList<ResourceRequest<?>>();
		for (int i = 1; i <= nKeys; i++) {
			int keyValue = initValue;
			if (valueVariation > 0)
				keyValue += uniformRandom.nextInt(valueVariation);
			CRDTIdentifier crdtId = new CRDTIdentifier(table, "" + i);
			counterRequests.add(new CounterReservation(ownerId, crdtId, 0));
			if (i % transactionSize == 0 || i == nKeys) {
				// System.out.println("Start transaction " + i);
				stub.beginTxn();
				while (counterRequests.size() > 0) {
					ResourceRequest<Integer> request = (ResourceRequest<Integer>) counterRequests.remove(0);
					// System.out.println("Increment " + request);
					increment(request.getResourceId(), keyValue, stub, ownerId);
				}
				counterRequests.clear();
				stub.endTxn();
			}
		}
		System.out.println("Finished initializing Counters");
		if (!Args.contains("-startDC") && !Args.contains("-dontExit"))
			System.exit(0);
	}

	private static void increment(CRDTIdentifier id, int amount, Indigo stub, String siteId) throws SwiftException {
		BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
		x.increment(amount, siteId);
	}

	public static void startSequencer() {
		System.out.printf("Start DataCenter: %s", DC_ID);
		DC_ID = Args.valueOf("-siteId", "X");
		MASTER_ID = Args.valueOf("-master", "X");
		int sequencerPort = Args.valueOf("-seqPort", 31001);
		String[] servers = Args.valueOf("-server", new String[]{"tcp://*:32001/"});
		String[] otherSequencers = Args.valueOf("-sequencers", new String[]{});
		TestsUtil.startSequencer(DC_ID, MASTER_ID, sequencerPort, servers, otherSequencers);
	}

	public static void startServer() {
		System.out.printf("Start DataCenter: %s", DC_ID);
		DC_ID = Args.valueOf("-siteId", "X");
		MASTER_ID = Args.valueOf("-master", "X");
		int serverPort = Args.valueOf("-srvPort", 32001);
		int serverPortForSequencer = Args.valueOf("-srvPortForSeq", 33001);
		int dhtPort = Args.valueOf("-dhtPort", 34001);
		int pubSubPort = Args.valueOf("-pubSubPort", 35001);
		int indigoPort = Args.valueOf("-indigoPort", 36001);
		String sequencerUrl = Args.valueOf("-sequencerUrl", "tcp://*:31001/");
		String[] otherServers = Args.valueOf("-servers", new String[]{});
		String redBlueUrl = Args.valueOf("-redblue", (String) null);

		if (redBlueUrl == null) {
			TestsUtil.startServer(DC_ID, MASTER_ID, serverPort, dhtPort, pubSubPort, indigoPort, serverPortForSequencer, sequencerUrl, otherServers);
		} else {
			TestsUtil.startServerRedBlue(DC_ID, MASTER_ID, serverPort, dhtPort, pubSubPort, indigoPort, serverPortForSequencer, sequencerUrl, otherServers, redBlueUrl);
		}
	}

	public static void startDC1Sequencer() {
		System.out.printf("Start DataCenter: %s", DC_ID);
		DC_ID = Args.valueOf("-siteId", "X");
		MASTER_ID = Args.valueOf("-master", "X");
		int sequencerPort = Args.valueOf("-seqPort", 31001);
		String serverUrl = Args.valueOf("-server", "tcp://*:32001/");
		String[] otherSequencers = Args.valueOf("-sequencers", new String[]{});

		TestsUtil.startSequencer(DC_ID, MASTER_ID, sequencerPort, new String[]{serverUrl}, otherSequencers);
	}

	public static void startDC1Server() {
		System.out.printf("Start DataCenter: %s", DC_ID);
		DC_ID = Args.valueOf("-siteId", "X");
		MASTER_ID = Args.valueOf("-master", "X");
		int serverPort = Args.valueOf("-srvPort", 32001);
		int serverPortForSequencer = Args.valueOf("-sFs", 33001);
		int dhtPort = Args.valueOf("-dhtPort", 34001);
		int pubSubPort = Args.valueOf("-pubSubPort", 35001);
		int indigoPort = Args.valueOf("-indigoPort", 36001);
		String sequencerUrl = Args.valueOf("-sequencerUrl", "tcp://*:31001/");
		String[] otherServers = Args.valueOf("-servers", new String[]{});
		String redBlueUrl = Args.valueOf("-redblue", (String) null);

		if (redBlueUrl == null) {
			TestsUtil.startServer(DC_ID, MASTER_ID, serverPort, dhtPort, pubSubPort, indigoPort, serverPortForSequencer, sequencerUrl, otherServers);
		} else {
			TestsUtil.startServerRedBlue(DC_ID, MASTER_ID, serverPort, dhtPort, pubSubPort, indigoPort, serverPortForSequencer, sequencerUrl, otherServers, redBlueUrl);
		}
	}

	public static void startDC2Sequencer() {
		System.out.printf("Start DataCenter: %s", DC_ID);
		DC_ID = Args.valueOf("-siteId", "X");
		MASTER_ID = Args.valueOf("-master", "X");
		int sequencerPort = Args.valueOf("-seqPort", 31002);
		String serverUrl = Args.valueOf("-server", "tcp://*:32001/");
		String[] otherSequencers = Args.valueOf("-sequencers", new String[]{});

		TestsUtil.startSequencer(DC_ID, MASTER_ID, sequencerPort, new String[]{serverUrl}, otherSequencers);
	}

	public static void startDC2Server() {
		System.out.printf("Start DataCenter: %s", DC_ID);
		DC_ID = Args.valueOf("-siteId", "X");
		MASTER_ID = Args.valueOf("-master", "X");
		int serverPort = Args.valueOf("-srvPort", 32002);
		int serverPortForSequencer = Args.valueOf("-sFs", 33002);
		int dhtPort = Args.valueOf("-dhtPort", 34002);
		int pubSubPort = Args.valueOf("-pubSubPort", 35002);
		int indigoPort = Args.valueOf("-indigoPort", 36002);
		String sequencerUrl = Args.valueOf("-sequencerUrl", "tcp://*:31002/");
		String[] otherServers = Args.valueOf("-servers", new String[]{});
		String redBlueUrl = Args.valueOf("-redblue", (String) null);

		if (redBlueUrl == null) {
			TestsUtil.startServer(DC_ID, MASTER_ID, serverPort, dhtPort, pubSubPort, indigoPort, serverPortForSequencer, sequencerUrl, otherServers);
		} else {
			TestsUtil.startServerRedBlue(DC_ID, MASTER_ID, serverPort, dhtPort, pubSubPort, indigoPort, serverPortForSequencer, sequencerUrl, otherServers, redBlueUrl);
		}
	}

	public static void startDC3Sequencer() {
		System.out.printf("Start DataCenter: %s", DC_ID);
		DC_ID = Args.valueOf("-siteId", "X");
		MASTER_ID = Args.valueOf("-master", "X");
		int sequencerPort = Args.valueOf("-seqPort", 31003);
		String serverUrl = Args.valueOf("-server", "tcp://*:32001/");
		String[] otherSequencers = Args.valueOf("-sequencers", new String[]{});

		TestsUtil.startSequencer(DC_ID, MASTER_ID, sequencerPort, new String[]{serverUrl}, otherSequencers);
	}

	public static void startDC3Server() {
		System.out.printf("Start DataCenter: %s", DC_ID);
		DC_ID = Args.valueOf("-siteId", "X");
		MASTER_ID = Args.valueOf("-master", "X");
		int serverPort = Args.valueOf("-srvPort", 32003);
		int serverPortForSequencer = Args.valueOf("-sFs", 33003);
		int dhtPort = Args.valueOf("-dhtPort", 34003);
		int pubSubPort = Args.valueOf("-pubSubPort", 35003);
		int indigoPort = Args.valueOf("-indigoPort", 36003);
		String sequencerUrl = Args.valueOf("-sequencerUrl", "tcp://*:31003/");
		String[] otherServers = Args.valueOf("-servers", new String[]{});

		TestsUtil.startServer(DC_ID, MASTER_ID, serverPort, dhtPort, pubSubPort, indigoPort, serverPortForSequencer, sequencerUrl, otherServers);
	}

	public static void main(String[] args) {
		try {
			Args.use(args);
			System.err.println(Arrays.asList(args));
			nKeys = Args.valueOf("-nKeys", 1);
			DC_ID = Args.valueOf("-siteId", "X");
			table = Args.valueOf("-table", "COUNTER_A");
			DC_ADDRESS = Args.valueOf("-srvAddress", "tcp://*/36001/");
			int nThreads = Args.valueOf("-threads", 1);
			int maxThinkTime = Args.valueOf("-maxThinkTime", 50);
			String distributionName = Args.valueOf("-distribution", "uniform");

			if (nKeys > 1) {
				if (distributionName.equals("zipf")) {
					int exponent = Args.valueOf("-exponent", 1);
					distribution = new ZipfDistribution(nKeys, exponent);
				} else if (distributionName.equals("uniform")) {
					distribution = new UniformIntegerDistribution(1, nKeys);
				}

			}

			if (Args.contains("-startSeq1")) {
				Thread dc = new Thread(new Runnable() {

					@Override
					public void run() {
						startDC1Sequencer();
					}
				});
				dc.setDaemon(false);
				dc.start();
			}

			if (Args.contains("-startSrv1")) {
				Thread dc = new Thread(new Runnable() {

					@Override
					public void run() {
						startDC1Server();
					}
				});
				dc.setDaemon(false);
				dc.start();

			}

			if (Args.contains("-startSeq2")) {
				Thread dc = new Thread(new Runnable() {

					@Override
					public void run() {
						startDC2Sequencer();
					}
				});
				dc.start();
			}

			if (Args.contains("-startSrv2")) {
				Thread dc = new Thread(new Runnable() {

					@Override
					public void run() {
						startDC2Server();
					}
				});
				dc.start();
			}

			if (Args.contains("-startSeq3")) {
				Thread dc = new Thread(new Runnable() {

					@Override
					public void run() {
						startDC3Sequencer();
					}
				});
				dc.start();
			}

			if (Args.contains("-startSrv3")) {
				Thread dc = new Thread(new Runnable() {

					@Override
					public void run() {
						startDC3Server();
					}
				});
				dc.start();
			}

			if (Args.contains("-startServer")) {
				Thread dc = new Thread(new Runnable() {

					@Override
					public void run() {
						startServer();
					}
				});
				dc.start();
			}

			if (Args.contains("-startSequencer")) {
				Thread sequencer = new Thread(new Runnable() {

					@Override
					public void run() {
						startSequencer();
					}
				});
				sequencer.start();
			}

			if (Args.contains("-init")) {
				int initValue = Args.valueOf("-initValue", 1000);
				int valueVariation = Args.valueOf("-valueVar", 0);
				Indigo stub = RemoteIndigo.getInstance(DC_ADDRESS);
				final String dc_id;
				if (Args.contains("-weak")) {
					dc_id = "GLOBAL";
				} else {
					dc_id = DC_ID;
				}
				initCounters(nKeys, table, initValue, valueVariation, dc_id, stub);
			}

			initLogger();

			if (Args.contains("-run")) {
				if (Args.contains("-shepard")) {
					PatientShepard.sheepJoinHerd(Args.valueOf("-shepard", ""));
				};
				int nWrites = Args.valueOf("-nWrites", 1);
				int nReads = Args.valueOf("-nReads", 0);
				decrementCycleNThreads(nThreads, maxThinkTime, nWrites, nReads);
			}

		} catch (SwiftException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
		}

	}

	static void initLogger() {
		Logger logger = Logger.getLogger(resultsLogName);
		profiler = Profiler.getInstance();
		if (logger.isLoggable(Level.FINEST)) {
			FileHandler fileTxt;
			try {
				String resultsDir = Args.valueOf("-results_dir", ".");
				String siteId = Args.valueOf("-siteId", "GLOBAL");
				String suffix = Args.valueOf("-fileNameSuffix", "");
				fileTxt = new FileHandler(resultsDir + "/micro_benchmark_results" + "_" + siteId + suffix + ".log");
				fileTxt.setFormatter(new java.util.logging.Formatter() {
					@Override
					public String format(LogRecord record) {
						return record.getMessage() + "\n";
					}
				});
				logger.addHandler(fileTxt);
				profiler.printMessage(resultsLogName, TestsUtil.dumpArgs());
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		profiler.printHeaderWithCustomFields(resultsLogName, "VALUE", "SUCCESS", "AVAILABLE", "N_WRITES", "N_READS");

	}
}
