package swift.indigo.proto;

import static swift.indigo.IndigoResourceManager.sortedRequests;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.indigo.IndigoResourceManager;
import swift.indigo.ResourceRequest;
import sys.utils.Threading;

public class AcquireResourcesReply {

    public enum AcquireReply {
        IMPOSSIBLE, YES, NO
    }

    long serial;

    protected Timestamp timestamp;
    protected CausalityClock snapshot;
    protected Collection<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
    protected AcquireReply status;

    transient ResourceRequest<?>[] requests;
    // transient boolean[] durable;
    transient Timestamp cltTimestamp;

    /**
     * Fake constructor for Kryo serialization. Do NOT use.
     */
    public AcquireResourcesReply() {
    }

    public AcquireResourcesReply(boolean dummy) {
        this.status = AcquireReply.NO;
        if (dummy != false)
            throw new RuntimeException("Expected false...");
    }

    public AcquireResourcesReply(AcquireReply status, CausalityClock snapshot) {
        this.status = status;
        this.snapshot = snapshot;
        this.objectUpdateGroups = Collections.emptyList();
    }

    public AcquireResourcesReply(Timestamp cltTimestamp, Timestamp timestamp, CausalityClock snapshot,
            Collection<CRDTObjectUpdatesGroup<?>> objectUpdateGroups, Collection<ResourceRequest<?>> requests) {
        this.status = AcquireReply.YES;
        this.snapshot = snapshot;
        this.timestamp = timestamp;
        this.cltTimestamp = cltTimestamp;
        this.objectUpdateGroups = objectUpdateGroups;
        this.requests = sortedRequests(requests);
        // this.durable = new boolean[this.requests.length];
    }

    // Used for weak consistency emulation...
    public AcquireResourcesReply(long serial, CausalityClock currentClockEstimate) {
        this.serial = serial;
        this.status = AcquireReply.YES;
        this.timestamp = null;
        this.snapshot = currentClockEstimate;
        this.objectUpdateGroups = Collections.emptyList();
    }

    public boolean matches(Timestamp clTimestamp) {
        return this.cltTimestamp.equals(cltTimestamp);
    }

    public long serial() {
        return serial;
    }

    public AcquireResourcesReply setSerial(long serial) {
        this.serial = serial;
        return this;
    }

    public Timestamp timestamp() {
        return timestamp;
    }

    public boolean acquiredResources() {
        return status == AcquireReply.YES;
    }

    public AcquireReply acquiredStatus() {
        return status;
    }

    public Collection<CRDTObjectUpdatesGroup<?>> operations() {
        return objectUpdateGroups;
    }

    /**
     * @return snapshot point when locks were acquired...
     */
    public CausalityClock getSnapshot() {
        return snapshot;
    }

    public void lockStuff() {
        Threading.lock(IndigoResourceManager.LOCKS_TABLE);
        // // System.err.println("Locking:" + Arrays.asList(locks) + ", " +
        // // Arrays.asList(counters));
        // for (Lock i : locks)
        // Threading.lock(i.id());
        // for (CounterReservation i : counters)
        // Threading.lock(i.getId());
        // // System.err.println("Locked:" + Arrays.asList(locks) + ", " +
        // // Arrays.asList(counters));
    }

    public void unlockStuff() {
        Threading.unlock(IndigoResourceManager.LOCKS_TABLE);
        // for (Lock i : locks)
        // Threading.unlock(i.id());
        // for (CounterReservation i : counters)
        // Threading.unlock(i.getId());
        // // System.err.println("UnLocked:" + Arrays.asList(locks) + ", " +
        // // Arrays.asList(counters));
    }

    public String toString() {
        return (status.equals(AcquireReply.YES) ? ("YES " + ((requests != null) ? Arrays.asList(requests) : "") + "  " + cltTimestamp)
                : ("NO"));
    }

    public ResourceRequest<?>[] getResourcesRequest() {
        return requests;
    }

    // public void setDurable(CRDTIdentifier crdt) {
    // for (int i = 0; i < requests.length; i++) {
    // durable[i] = true;
    // }
    // }
    //
    // public boolean allDurable() {
    // for (int i = 0; i < requests.length; i++) {
    // if (durable[i] != true) {
    // return false;
    // }
    // }
    // return true;
    // }

}
