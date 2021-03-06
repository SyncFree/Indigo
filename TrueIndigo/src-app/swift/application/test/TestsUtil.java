package swift.application.test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import swift.api.CRDTIdentifier;
import swift.crdt.BoundedCounterAsResource;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.ShareableLock;
import swift.exceptions.SwiftException;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.IndigoServer;
import swift.indigo.LockReservation;
import swift.indigo.ResourceRequest;
import swift.indigo.remote.IndigoImpossibleException;
import sys.utils.Args;

public class TestsUtil {

	public static void startSequencer(String siteId, String masterId, int sequencerPort, String[] servers, String[] otherSequencers) {
		List<String> argsSeq = new LinkedList<String>();
		argsSeq.addAll(Arrays.asList(new String[]{"-master", masterId, "-siteId", siteId, "-url", "tcp://*:" + sequencerPort}));
		argsSeq.add("-server");
		argsSeq.addAll(Arrays.asList(servers));
		argsSeq.add("-sequencers");
		argsSeq.addAll(Arrays.asList(otherSequencers));
		IndigoSequencerAndResourceManager.main(argsSeq.toArray(new String[0]));
	}

	public static void startServer(String siteId, String masterId, int serverPort, int DHTPort, int pubSubPort, int indigoPort, int serverPort4Seq, String sequencerUrl, String[] otherServers) {
		List<String> argsServer = new LinkedList<String>();
		argsServer.addAll(Arrays.asList(new String[]{"-siteId", siteId, "-url", "tcp://*:" + serverPort, "-sequencer", sequencerUrl, "-url4Seq", "tcp://*:" + serverPort4Seq + "/", "-dht", "tcp://*:" + DHTPort, "-pubsub",
				"tcp://*:" + pubSubPort, "-indigo", "" + "tcp://*:" + indigoPort, "-servers"}));

		List<String> servers = new ArrayList<>();
		for (String server : otherServers) {
			if (!server.contains(siteId))
				servers.add(server);
		}
		argsServer.addAll(servers);
		IndigoServer.main(argsServer.toArray(new String[]{}));
	}

	public static void startServerRedBlue(String siteId, String masterId, int serverPort, int DHTPort, int pubSubPort, int indigoPort, int serverPort4Seq, String sequencerUrl, String[] otherServers, String redSeqUrl) {
		List<String> argsServer = new LinkedList<String>();
		argsServer.addAll(Arrays.asList(new String[]{"-siteId", siteId, "-url", "tcp://*:" + serverPort, "-sequencer", sequencerUrl, "-url4Seq", "tcp://*:" + serverPort4Seq + "/", "-dht", "tcp://*:" + DHTPort, "-pubsub",
				"tcp://*:" + pubSubPort, "-indigo", "" + "tcp://*:" + indigoPort, "-servers"}));

		List<String> servers = new ArrayList<>();
		for (String server : otherServers) {
			servers.add(server);
		}
		System.err.println("-----------SERVERS:" + servers);
		argsServer.addAll(servers);
		argsServer.add("-redblue");
		argsServer.add(redSeqUrl);
		System.out.println("\n\nARGS " + argsServer);
		IndigoServer.main(argsServer.toArray(new String[]{}));
	}

	public static void increment(CRDTIdentifier id, int units, Indigo stub, String siteId) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
		x.increment(units, siteId);
		stub.endTxn();
	}

	public static boolean decrement(CRDTIdentifier id, int units, Indigo stub, String siteId) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation(siteId, id, units));
		boolean result;
		try {
			stub.beginTxn(resources);
			BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);

			result = x.decrement(units, siteId);
		} catch (IndigoImpossibleException e) {
			result = false;
		} finally {
			stub.endTxn();
		}
		return result;
	}
	public static int getValue(CRDTIdentifier id, Indigo stub) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
		stub.endTxn();
		return x.getValue();
	}

	public static boolean getValueDecrement(CRDTIdentifier id, int units, Indigo stub, String siteId) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		resources.add(new CounterReservation(siteId, id, units));
		boolean result = false;
		try {
			stub.beginTxn(resources);
			BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
			if (x.getValue() > 0) {
				result = x.decrement(units, siteId);
			}
		} catch (IndigoImpossibleException e) {
			result = false;
		} finally {
			stub.endTxn();
		}
		return result;
	}

	public static void compareValue(CRDTIdentifier id, int expected, Indigo stub) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
		assertEquals((Integer) expected, x.getValue());
		stub.endTxn();
	}

	public static String dumpArgs() {
		StringBuilder result = new StringBuilder();
		for (String s : Args.getCurrent()) {
			result.append(s);
			result.append("\n");
		}
		return result.toString();

	}

	public static void doOp(String siteId, CRDTIdentifier idLock, CRDTIdentifier idValue, String value, ShareableLock lock, Indigo stub, long sleepBeforeCommit) throws Exception {
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		LockReservation request = new LockReservation(siteId, idLock, lock);
		resources.add(request);
		stub.beginTxn(resources);
		LWWRegisterCRDT<String> register = (LWWRegisterCRDT<String>) stub.get(idValue, true, LWWRegisterCRDT.class);
		register.set(value);
		Thread.sleep(sleepBeforeCommit);
		stub.endTxn();

	}

	public static void doThreadOp(final String siteId, final CRDTIdentifier idLock, CRDTIdentifier idValue, final String value, final ShareableLock lock, final Indigo stub, final long sleepBeforeCommit) {
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					doOp(siteId, idLock, idValue, value, lock, stub, sleepBeforeCommit);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

}
