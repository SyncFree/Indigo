package swift.indigo.proto;

import java.util.Collection;
import java.util.Collections;

import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.indigo.ResourceRequest;

public class AcquireResourcesReply {

	public enum AcquireReply {
		IMPOSSIBLE, YES, NO, RELEASED, REPEATED, NO_RESOURCES
	}

	long serial;

	protected Timestamp timestamp;
	protected CausalityClock snapshot;
	protected Collection<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
	protected AcquireReply status;
	protected Collection<CRDTIdentifier> impossible;

	transient Collection<ResourceRequest<?>> requests;
	transient Timestamp cltTimestamp;

	private long time;

	/**
	 * Fake constructor for Kryo serialization. Do NOT use.
	 */
	public AcquireResourcesReply() {
	}

	public AcquireResourcesReply(boolean dummy) {
		this.time = System.currentTimeMillis();
		this.status = AcquireReply.NO;
		if (dummy != false)
			throw new RuntimeException("Expected false...");
	}

	public AcquireResourcesReply(AcquireReply status, CausalityClock snapshot) {
		this.time = System.currentTimeMillis();
		this.status = status;
		this.snapshot = snapshot;
		this.objectUpdateGroups = Collections.emptyList();
	}

	public AcquireResourcesReply(AcquireReply status, CausalityClock snapshot, Collection<CRDTIdentifier> impossible) {
		this(status, snapshot);
		this.impossible = impossible;
	}

	public AcquireResourcesReply(Timestamp cltTimestamp, Timestamp timestamp, CausalityClock snapshot, Collection<CRDTObjectUpdatesGroup<?>> objectUpdateGroups, Collection<ResourceRequest<?>> requests) {
		this.time = System.currentTimeMillis();
		this.status = AcquireReply.YES;
		this.snapshot = snapshot;
		this.timestamp = timestamp;
		this.cltTimestamp = cltTimestamp;
		this.objectUpdateGroups = objectUpdateGroups;
		this.requests = requests;
	}

	// Used for weak consistency emulation...
	public AcquireResourcesReply(long serial, CausalityClock currentClockEstimate) {
		this.time = System.currentTimeMillis();
		this.serial = serial;
		this.status = AcquireReply.YES;
		this.timestamp = null;
		this.snapshot = currentClockEstimate;
		this.objectUpdateGroups = Collections.emptyList();
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
		return status == AcquireReply.YES || status == AcquireReply.NO_RESOURCES || status == AcquireReply.RELEASED;
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

	public String toString() {
		return "STATUS " + status + " TS: " + timestamp;
	}
	public Collection<ResourceRequest<?>> getResourcesRequest() {
		return requests;
	}

	public boolean isReleased() {
		return status.equals(AcquireReply.RELEASED);
	}

	public void setReleased() {
		status = AcquireReply.RELEASED;
	}

	public boolean isImpossible() {
		return status.equals(AcquireReply.IMPOSSIBLE);
	}

	public long getPhysicalClock() {
		return time;
	}

	public Collection<CRDTIdentifier> getImpossibleIds() {
		return impossible;
	}

}
