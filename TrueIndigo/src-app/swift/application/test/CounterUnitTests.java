package swift.application.test;

import static org.junit.Assert.assertEquals;
import static sys.Context.Networking;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Semaphore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import swift.api.CRDTIdentifier;
import swift.crdt.BoundedCounterAsResource;
import swift.exceptions.SwiftException;
import swift.indigo.CounterReservation;
import swift.indigo.Defaults;
import swift.indigo.Indigo;
import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.IndigoServer;
import swift.indigo.ResourceRequest;
import swift.indigo.remote.RemoteIndigo;

public class CounterUnitTests {

    static Indigo stub1, stub2;
    static String hostname = "X";
    static String serversAdresses = "localhost";
    static String table = "COUNTER";
    static char key = 'A';

    @Before
    public void init() throws InterruptedException, SwiftException {
        key++;
        if (stub1 == null) {
            IndigoSequencerAndResourceManager.main(new String[] { "-name", hostname, "-severs", serversAdresses });
            IndigoServer.main(new String[0]);
            Thread.sleep(1000);
            stub1 = RemoteIndigo.getInstance(Networking.resolve("localhost", Defaults.REMOTE_INDIGO_URL));
            stub2 = RemoteIndigo.getInstance(Networking.resolve("localhost", Defaults.REMOTE_INDIGO_URL));
        }

        initKey(stub1);
    }

    public void initKey(Indigo stub) throws SwiftException {
        List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
        resources.add(new CounterReservation(hostname, new CRDTIdentifier(table, "" + key), 0));

        stub.beginTxn(resources);
        stub.endTxn();
        stub.beginTxn();
        stub.get(new CRDTIdentifier(table, "" + key), false, BoundedCounterAsResource.class);
        stub.endTxn();

    }

    @Test
    public void incrementAndRead() throws SwiftException {
        increment("" + key, 10, stub1);
        compareValue("" + key, 10, stub1);
    }

    @Test
    public void twoIncrementAndRead() throws SwiftException {
        increment("" + key, 10, stub1);
        increment("" + key, 10, stub1);
        compareValue("" + key, 20, stub1);
    }

    @Test
    public void decrementAborts() throws SwiftException, InterruptedException {
        increment("" + key, 10, stub1);
        assertEquals(true, decrement("" + key, 10, stub1));
        Thread.sleep(5000);
        assertEquals(false, decrement("" + key, 10, stub1));
    }

    @Test
    public void decrementStillAborts() throws SwiftException {
        increment("" + key, 9, stub1);
        assertEquals(false, decrement("" + key, 10, stub1));
    }

    @Test
    public void decrementSucceeds() throws SwiftException {
        increment("" + key, 10, stub1);
        assertEquals(true, decrement("" + key, 10, stub1));
    }

    @Test
    public void decrementCycle() throws SwiftException {
        int count = 50;
        increment("" + key, count, stub1);
        for (int i = 0; i < count; i++) {
            decrement("" + key, 1, stub1);
            System.out.println(getValue("" + key, stub1));
        }
        compareValue("" + key, 0, stub1);
    }

    @Test
    public void decrementCycleTwotThreads() throws SwiftException, InterruptedException, BrokenBarrierException {
        int count = 50;
        increment("" + key, count, stub1);
        Semaphore sem = new Semaphore(2);
        sem.acquire(2);
        new Thread(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < count / 2; i++) {
                        decrement("" + key, 1, stub1);
                        System.out.println(getValue("" + key, stub1));
                    }
                    sem.release();
                } catch (SwiftException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < count / 2; i++) {
                        decrement("" + key, 1, stub2);
                        // System.out.println(getValue("" + key, stub2));
                    }
                    sem.release();
                } catch (SwiftException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        sem.acquire();
        Thread.sleep(1000);
        compareValue("" + key, 0, stub1);

    }

    @Test
    public void waitAndSucceed() throws SwiftException, InterruptedException {
        increment("" + key, 10, stub1);

        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    decrement("" + key, 10, stub1);
                    System.out.println("Decrement and sleep");
                    Thread.sleep(500);
                    increment("" + key, 10, stub1);
                    System.out.println("First thread increments and finish");
                } catch (SwiftException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }).start();

        Thread.sleep(500);

        new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    decrement("" + key, 8, stub2);
                    System.out.println("Second thread succeeded");
                } catch (SwiftException e) {
                    e.printStackTrace();
                }

            }
        }).start();

        Thread.sleep(5000000);
    }

    public void increment(String key, int units, Indigo stub) throws SwiftException {
        stub.beginTxn();
        BoundedCounterAsResource x = stub.get(new CRDTIdentifier(table, key), false, BoundedCounterAsResource.class);
        x.increment(units, hostname);
        stub.endTxn();

    }

    public boolean decrement(String key, int units, Indigo stub) throws SwiftException {
        List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
        resources.add(new CounterReservation(hostname, new CRDTIdentifier(table, key), units));

        stub.beginTxn(resources);
        BoundedCounterAsResource x = stub.get(new CRDTIdentifier(table, key), false, BoundedCounterAsResource.class);

        boolean result = x.decrement(units, hostname);

        stub.endTxn();
        return result;

    }

    public void compareValue(String key, int expected, Indigo stub) throws SwiftException {
        stub.beginTxn();
        BoundedCounterAsResource x = stub.get(new CRDTIdentifier(table, key), false, BoundedCounterAsResource.class);

        assertEquals((Integer) expected, x.getValue());

        stub.endTxn();

    }

    public int getValue(String key, Indigo stub) throws SwiftException {
        stub.beginTxn();
        BoundedCounterAsResource x = stub.get(new CRDTIdentifier(table, key), false, BoundedCounterAsResource.class);
        stub.endTxn();
        return x.getValue();
    }

    @After
    public void close() {
        // Should Stop the nodes.
    }

}
