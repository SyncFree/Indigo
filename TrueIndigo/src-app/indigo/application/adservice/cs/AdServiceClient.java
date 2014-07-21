package indigo.application.adservice.cs;

import static java.lang.System.exit;
import indigo.application.adservice.AdServiceBenchmark;
import indigo.application.adservice.AdServiceOps;
import indigo.application.adservice.Commands;
import indigo.application.adservice.Results;

import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import swift.indigo.IndigoSequencerAndResourceManager;
import sys.net.api.Endpoint;
import sys.net.impl.Networking;
import sys.utils.Args;

public class AdServiceClient extends AdServiceBenchmark {
    private static Logger Log = Logger.getLogger(AdServiceClient.class.getName());

    Endpoint server;

    void init(String[] args) {
        server = Networking.resolve(Args.valueOf(args, "-server", "localhost"), AdServiceServer.APPSERVER_PORT);
    }

    @Override
    public Results runCommandLine(int sessionId, AdServiceOps adServiceClient, String cmdLine) {
        String[] toks = cmdLine.split(";");
        final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
        switch (cmd) {
        case VIEW_AD:
            long start = System.currentTimeMillis();
            AppReply reply = endpointFor(sessionId).request(server, new AppRequest(sessionId, cmdLine));
            long end = System.currentTimeMillis();
            return new RemoteResults(reply.payload + String.format(",%s,%s,%s", start, end, end - start));
        default:
            Log.warning("Can't parse command line :" + cmdLine);
            Log.warning("Exiting...");
            System.exit(1);
        }
        return new RemoteResults(cmd.toString());
    }

    Map<Integer, RpcEndpoint> endpoints = new ConcurrentHashMap<Integer, RpcEndpoint>();

    RpcEndpoint endpointFor(int sessionId) {
        RpcEndpoint res = endpoints.get(sessionId);
        if (res == null)
            endpoints.put(sessionId, res = Networking.rpcConnect().toDefaultService());
        return res;
    }

    public static void main(String[] args) {
        sys.Sys.init();

        AdServiceClient client = new AdServiceClient();
        if (args.length == 0) {

            IndigoSequencerAndResourceManager.main(new String[] { "-name", "X0" });

            args = new String[] { "-server", "localhost", "-name", "X0", "-threads", "1" };

            AdServiceServer.main(args);

            client.init(args);
            client.initDB(args);
            client.doBenchmark(args);
            exit(0);
        }

        if (args[0].equals("-init")) {
            client.init(args);
            client.initDB(args);
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
