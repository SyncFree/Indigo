package swift.application.test;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.ShareableLock;
import swift.crdt.core.CRDTIdentifier;
import swift.exceptions.SwiftException;
import swift.indigo.Indigo;
import swift.indigo.IndigoAppServer;
import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.LockReservation;
import swift.indigo.ResourceRequest;

public class LockUnitTests {

    static Indigo stub;
    static String hostname = "X0";
    static String serversAdresses = "localhost";
    static String table = "REGISTER";
    static char key = 'A';

    @Before
    public void init() throws InterruptedException, SwiftException {
        key++;
        if (stub == null) {
            IndigoSequencerAndResourceManager.main(new String[] { "-name", hostname, "-severs", serversAdresses });
            IndigoAppServer.main(new String[0]);
            Thread.sleep(1000);
            stub = IndigoAppServer.getIndigo();
        }
        initKey();
    }

    public void initKey() throws SwiftException {
        List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
        resources.add(new LockReservation(hostname, new CRDTIdentifier("LOCK", "A"), ShareableLock.ALLOW));

        stub.beginTxn(resources);
        stub.endTxn();
        stub.beginTxn();
        stub.get(new CRDTIdentifier("LOCK", "A"), false, EscrowableTokenCRDT.class);
        stub.endTxn();

    }

    public static void doOp(String siteId, String key, String value, ShareableLock lock, long sleepBeforeCommit)
            throws Exception {
        List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
        LockReservation request = new LockReservation(siteId, new CRDTIdentifier("LOCK", "A"), lock);
        resources.add(request);

        stub.beginTxn(resources);
        LWWRegisterCRDT<String> register = stub.get(new CRDTIdentifier(table, key), true, LWWRegisterCRDT.class);
        register.set(value);

        Thread.sleep(sleepBeforeCommit);

        stub.endTxn();

    }

    public static void doThreadOp(final String siteId, final String key, final String value, final ShareableLock lock,
            final long sleepBeforeCommit) {
        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    doOp(siteId, key, value, lock, sleepBeforeCommit);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    // Gets an exclusive lock and executes the operation
    @Test
    public void simpleSetStringTest() throws Exception {
        doOp(hostname, "" + key, "A", ShareableLock.EXCLUSIVE_ALLOW, 0);
        getValue("" + key, "A");
    }

    @Test
    public void impossibleToGetLockTest() throws Exception {
        doThreadOp(hostname, "" + key, "VALUE", ShareableLock.ALLOW, 5000);

        // Request lock concurrently, while the first is active
        doThreadOp(hostname, "" + key, "VALUE", ShareableLock.FORBID, 0);
        doThreadOp(hostname, "" + key, "VALUE", ShareableLock.EXCLUSIVE_ALLOW, 0);

        Thread.sleep(12000);
    }

    public void getValue(String key, String expected) throws SwiftException {
        stub.beginTxn();
        LWWRegisterCRDT<String> x = stub.get(new CRDTIdentifier(table, key), false, LWWRegisterCRDT.class);

        assertEquals(expected, x.getValue());

        stub.endTxn();

    }

    @After
    public void close() {
        // Should Stop the nodes.
    }

}
