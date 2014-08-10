package swift.application.test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.IndigoServer;

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

}
