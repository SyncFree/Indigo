package indigo.application.benchmark;

import static sys.Context.Networking;
import swift.api.CRDTIdentifier;
import swift.exceptions.SwiftException;
import swift.indigo.Indigo;
import swift.indigo.remote.RemoteIndigo;

public class MicroBenchmarkTest {

	public static void main(String[] args) throws SwiftException {
		MicroBenchmark.initLogger();

		Indigo stub1 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001"));
		Indigo stub2 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36002"));
		Indigo stub3 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36003"));

		MicroBenchmark.getValueDecrement(new CRDTIdentifier("table", 1 + ""), 1, stub1, "US-EAST");
		MicroBenchmark.getValueDecrement(new CRDTIdentifier("table", 1 + ""), 1, stub2, "US-WEST");
		MicroBenchmark.getValueDecrement(new CRDTIdentifier("table", 1 + ""), 1, stub3, "EUROPE");

		MicroBenchmark.getValueDecrement(new CRDTIdentifier("table", 4999997 + ""), 1, stub1, "US-EAST");
		MicroBenchmark.getValueDecrement(new CRDTIdentifier("table", 1 + ""), 1, stub1, "US-EAST");

	}

}
