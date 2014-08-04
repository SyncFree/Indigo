package swift.application.test;

import static org.junit.Assert.assertEquals;
import static sys.Context.Networking;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import swift.api.CRDTIdentifier;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.ShareableLock;
import swift.exceptions.SwiftException;
import swift.indigo.Indigo;
import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.IndigoServer;
import swift.indigo.LockReservation;
import swift.indigo.ResourceRequest;
import swift.indigo.remote.RemoteIndigo;

public class IndigoMultipleServersTest {

    static boolean started;
    private static Indigo stub1;
    private static Indigo stub2;

    static String table = "LOCK";
    static char key = 'A';

    public static void startDC1Server(String siteId, int sequencerPort, int serverPort, int serverPort4Seq,
            int DHTPort, int pubSubPort, int indigoPort, String[] otherSequencers, String[] otherServers) {

        List<String> argsSeq = new LinkedList<String>();
        argsSeq.addAll(Arrays.asList(new String[] { "-siteId", siteId, "-url", "tcp://*:" + sequencerPort, "-server",
                "tcp://*:" + serverPort, "-sequencers" }));
        argsSeq.addAll(Arrays.asList(otherSequencers));
        IndigoSequencerAndResourceManager.main(argsSeq.toArray(new String[0]));

        List<String> argsServer = new LinkedList<String>();
        argsServer.addAll(Arrays.asList(new String[] { "-url", "tcp://*:" + serverPort, "-sequencer",
                "tcp://*:" + sequencerPort, "-url4seq", "" + serverPort4Seq, "-dht", "tcp://*:" + DHTPort, "-pubsub",
                "tcp://*:" + pubSubPort, "-indigo", "" + "tcp://*:" + indigoPort, "-servers" }));
        argsServer.addAll(Arrays.asList(otherServers));
        IndigoServer.main(argsServer.toArray(new String[0]));
    }

    @Before
    public void init2DC() {
        if (!started) {
            startDC1Server("DC_A", 31001, 32001, 33001, 34001, 35001, 36001, new String[] {
                    "tcp://*:" + 31001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/" }, new String[] { "tcp://*:" + 32002
                    + "/DC_B" });
            startDC1Server("DC_B", 31002, 32002, 33002, 34002, 35002, 36002, new String[] {
                    "tcp://*:" + 31001 + "/DC_A/", "tcp://*:" + 31002 + "/DC_B/" }, new String[] { "tcp://*:" + 32001
                    + "/DC_A" });
            started = true;

            stub1 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36001/DC_A/"));
            stub2 = RemoteIndigo.getInstance(Networking.resolve("tcp://*/36002/DC_B/"));
        }
    }

    @Test
    public void testReplication() throws SwiftException, InterruptedException {
        List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
        resources.add(new LockReservation("DC_A", new CRDTIdentifier("LOCK", "A"), ShareableLock.ALLOW));

        stub1.beginTxn(resources);
        stub1.endTxn();
        stub1.beginTxn();
        System.out.println(stub1.get(new CRDTIdentifier(table, "" + key), false, EscrowableTokenCRDT.class));
        stub1.endTxn();

        stub1.beginTxn();
        ShareableLock typeDC1 = stub1.get(new CRDTIdentifier(table, "" + key), false, EscrowableTokenCRDT.class)
                .getValue();
        stub1.endTxn();

        Thread.sleep(1000);
        stub2.beginTxn();
        ShareableLock typeDC2 = stub2.get(new CRDTIdentifier(table, "" + key), false, EscrowableTokenCRDT.class)
                .getValue();
        stub2.endTxn();

        assertEquals(typeDC1, typeDC2);

    }

}
