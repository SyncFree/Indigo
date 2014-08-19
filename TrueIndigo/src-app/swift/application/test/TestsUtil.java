package swift.application.test;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import swift.api.CRDTIdentifier;
import swift.crdt.BoundedCounterAsResource;
import swift.exceptions.SwiftException;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.IndigoServer;
import swift.indigo.ResourceRequest;
import swift.indigo.remote.IndigoImpossibleExcpetion;

public class TestsUtil {

	public static void startDC1Server(String siteId, int sequencerPort, int serverPort, int serverPort4Seq,
			int DHTPort, int pubSubPort, int indigoPort, String[] otherSequencers, String[] otherServers) {

		List<String> argsSeq = new LinkedList<String>();
		argsSeq.addAll(Arrays.asList(new String[]{"-siteId", siteId, "-url", "tcp://*:" + sequencerPort, "-server",
				"tcp://*:" + serverPort, "-sequencers"}));
		argsSeq.addAll(Arrays.asList(otherSequencers));
		IndigoSequencerAndResourceManager.main(argsSeq.toArray(new String[0]));

		List<String> argsServer = new LinkedList<String>();
		argsServer.addAll(Arrays.asList(new String[]{"-url", "tcp://*:" + serverPort, "-sequencer",
				"tcp://*:" + sequencerPort, "-url4seq", "" + serverPort4Seq, "-dht", "tcp://*:" + DHTPort, "-pubsub",
				"tcp://*:" + pubSubPort, "-indigo", "" + "tcp://*:" + indigoPort, "-servers"}));
		argsServer.addAll(Arrays.asList(otherServers));
		IndigoServer.main(argsServer.toArray(new String[0]));
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
		} catch (IndigoImpossibleExcpetion e) {
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

	public static void compareValue(CRDTIdentifier id, int expected, Indigo stub) throws SwiftException {
		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(id, false, BoundedCounterAsResource.class);
		assertEquals((Integer) expected, x.getValue());
		stub.endTxn();
	}

}
