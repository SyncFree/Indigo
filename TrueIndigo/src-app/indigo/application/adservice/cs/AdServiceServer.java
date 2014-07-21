package indigo.application.adservice.cs;

import static sys.net.api.Networking.Networking;
import indigo.application.adservice.AdServiceApp;
import indigo.application.adservice.AdServiceOps;
import indigo.application.adservice.Results;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.indigo.IndigoAppServer;
import sys.net.api.rpc.RpcHandle;
import sys.utils.Args;

public class AdServiceServer {
    private static Logger Log = Logger.getLogger(AdServiceServer.class.getName());

    public static int APPSERVER_PORT = 19999;

    public static void main(String[] args) {
        IndigoAppServer.main(args);

        final AdServiceApp app = new AdServiceApp();

        app.populateWorkloadFromConfig();

        final String siteId = Args.valueOf(args, "-name", "X0");

        Networking.rpcBind(APPSERVER_PORT).toService(0, new AppRequestHandler() {

            @Override
            public void onReceive(final RpcHandle handle, final AppRequest m) {
                String cmdLine = m.payload;
                String sessionId = handle.remoteEndpoint().toString() + "-" + m.sessionId;

                AdServiceOps session = getSession(sessionId, siteId);
                try {
                    if (Log.isLoggable(Level.INFO))
                        Log.info(sessionId + "   " + cmdLine);

                    long txnStart = System.currentTimeMillis();
                    Results res = app.runCommandLine(m.sessionId, session, cmdLine);
                    res.setSession(m.sessionId).setStartTime(txnStart);
                    if (Log.isLoggable(Level.INFO))
                        Log.info(sessionId + "   " + "OK");
                    handle.reply(new AppReply(res.getLogRecord()));
                } catch (Exception x) {
                    handle.reply(new AppReply("ERROR"));
                    x.printStackTrace();
                }
            }
        });

    }

    static AdServiceOps getSession(String sessionId, String siteId) {
        AdServiceOps res = sessions.get(sessionId);
        if (res == null)
            sessions.put(sessionId, res = new AdServiceOps(IndigoAppServer.getIndigo(), siteId));
        return res;
    }

    static Map<String, AdServiceOps> sessions = new ConcurrentHashMap<String, AdServiceOps>();
}
