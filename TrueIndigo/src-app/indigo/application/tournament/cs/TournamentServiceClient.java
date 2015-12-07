package indigo.application.tournament.cs;

import static java.lang.System.exit;
import static sys.Context.Networking;
import indigo.application.adservice.Results;
import indigo.application.tournament.reservations.TournamentServiceBenchmark;
import indigo.application.tournament.reservations.TournamentServiceOps;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import swift.indigo.IndigoSequencer;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.utils.Args;

public class TournamentServiceClient extends TournamentServiceBenchmark {
	private static Logger Log = Logger.getLogger(TournamentServiceClient.class.getName());

	Endpoint server;

	void init(String[] args) {
		server = Networking.resolve(Args.valueOf(args, "-server", "localhost"), "" + TournamentServiceServer.APPSERVER_PORT);
		// endpoint = Networking.rpcConnect().toDefaultService();
	}

	@Override
	public Results runCommandLine(int sessionId, TournamentServiceOps tournamentClient, String cmdLine) {
		long start = System.currentTimeMillis();
		AppReply reply = endpointFor(sessionId).request(server, new AppRequest(sessionId, cmdLine));
		long end = System.currentTimeMillis();
		return new RemoteResults(reply.payload + String.format(",%s,%s,%s", start, end, end - start));
	}

	Map<Integer, Service> endpoints = new ConcurrentHashMap<Integer, Service>();

	Service endpointFor(int sessionId) {
		Service res = endpoints.get(sessionId);
		if (res == null)
			endpoints.put(sessionId, res = Networking.stub());
		return res;
	}
	public static void main(String[] args) {

		TournamentServiceClient client = new TournamentServiceClient();
		if (args.length == 0) {

			IndigoSequencer.main(new String[]{"-name", "INIT"});

			args = new String[]{"-server", "localhost"};

			TournamentServiceServer.main(args);

			client.init(args);
			client.initDB(args);
			client.doBenchmark(args);
			exit(0);
		}

		if (args[0].equals("-run")) {
			client.init(args);
			client.doBenchmark(args);
			exit(0);
		}
	}

	static class RemoteResults implements Results {

		String results;

		RemoteResults(String reply) {
			this.results = reply;
		}

		@Override
		public Results setStartTime(long start) {
			return this;
		}

		@Override
		public Results setSession(int sessionId) {
			return this;
		}

		public String getLogRecord() {
			return results;
		}

		@Override
		public void logTo(PrintStream out) {
			out.println(results);
		}

	}
}
