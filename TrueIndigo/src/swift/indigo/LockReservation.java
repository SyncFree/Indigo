package swift.indigo;

import swift.api.CRDTIdentifier;
import swift.clocks.Timestamp;
import swift.crdt.ShareableLock;
import sys.utils.Threading;

public class LockReservation implements ResourceRequest<ShareableLock> {

	public CRDTIdentifier resourceId;
	public ShareableLock type;
	public String requesterId;

	// ATTENTION: The timestamp is used in the hashcode but it might not be set.
	// The hashcode function should only be called when it is set (after the
	// lock is obtained)
	private Timestamp ts;

	LockReservation() {
	}

	public LockReservation(String resourceId, ShareableLock type) {
		this("app", new CRDTIdentifier("/locks/", resourceId), type);
	}

	public LockReservation(String requesterId, CRDTIdentifier resourceId, ShareableLock type) {
		this.requesterId = requesterId;
		this.resourceId = resourceId;
		this.type = type;
	}

	@Override
	public CRDTIdentifier getResourceId() {
		return resourceId;
	}

	@Override
	public ShareableLock getResource() {
		return type;
	}

	@Override
	public String getRequesterId() {
		return requesterId;
	}

	public String toString() {
		return "{ Requester: " + requesterId + " id: " + resourceId + ", type: " + type + "}";
	}

	public int hashCode() {
		return type.hashCode() ^ resourceId.hashCode() + ts.hashCode();
	}

	private boolean equals(LockReservation other) {
		return type.equals(other.type) && resourceId.equals(other.resourceId);
	}

	public boolean equals(Object other) {
		return other != null && equals((LockReservation) other);
	}

	@Override
	public int compareTo(ResourceRequest<ShareableLock> o) {
		if (o instanceof LockReservation) {
			LockReservation other = (LockReservation) o;
			int cmp = resourceId.compareTo(other.resourceId);
			return cmp != 0 ? cmp : type.ordinal() - other.type.ordinal();
		} else {
			// LockReservation has more priority than any other resource type
			return -1;
		}

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

	@Override
	public void setClientTs(Timestamp ts) {
		this.ts = ts;

	}

}
