package swift.application.test;

import java.util.HashSet;

import swift.api.CRDTIdentifier;
import swift.crdt.BoundedCounterAsResource;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.IndigoAppServer;
import swift.indigo.ResourceRequest;
import sys.utils.Threading;

public class IndigoCounterTest {

	static Indigo stub;
	static String hostname = "X0";

	public static void init(String siteId) throws Exception {

		HashSet<ResourceRequest<?>> resources = new HashSet<ResourceRequest<?>>();
		resources.add(new CounterReservation(siteId, new CRDTIdentifier("COUNTER", "A"), 0));
		System.err.println("STARTING------>" + siteId + "\n\n\n");

		stub.beginTxn(resources);
		stub.endTxn();

		stub.beginTxn();
		BoundedCounterAsResource x = stub.get(new CRDTIdentifier("COUNTER", "A"), false, BoundedCounterAsResource.class);
		x.produce(siteId, 100);
		stub.endTxn();

		System.err.println("ENDING------>" + siteId + "\n\n\n");
	}

	public static void decrement(String siteId, int amount) throws Exception {
		HashSet<ResourceRequest<?>> resources = new HashSet<ResourceRequest<?>>();
		CounterReservation request = new CounterReservation(siteId, new CRDTIdentifier("COUNTER", "A"), amount);
		resources.add(request);

		System.err.println("STARTING------>" + siteId + "\n\n\n");

		stub.beginTxn(resources);
		BoundedCounterAsResource x = stub.get(new CRDTIdentifier("COUNTER", "A"), false, BoundedCounterAsResource.class);
		if (x.getValue() > 0) {
			x.consume(siteId, request);
		} else {
			System.out.println("Value is 0");
		}
		stub.endTxn();

		System.err.println("ENDING------>" + siteId + "\n\n\n");
	}

	public static void test() {
		try {
			Threading.sleep(5000);
			stub = IndigoAppServer.getIndigo();
			init(hostname);

			// Threading.sleep(2000);
			decrement(hostname, 100);
			// decrement("localhost", 1);

		} catch (Exception x) {
			x.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		IndigoAppServer.main(args);

		test();

		System.err.println("Great. Nothing to do...");
	}
}
