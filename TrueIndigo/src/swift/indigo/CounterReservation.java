package swift.indigo;

import swift.api.CRDTIdentifier;
import swift.clocks.Timestamp;
import sys.utils.Threading;

public class CounterReservation implements ResourceRequest<Integer> {

	private CRDTIdentifier resourceId;
	private int resource;
	private String requesterId;

	// ATTENTION: The timestamp is used in the hashcode but it might not be set.
	// The hashcode function should only be called when it is set (after the
	// counter is reserved)
	private Timestamp ts;

	public CounterReservation() {

	}

	public CounterReservation(CRDTIdentifier id, int amount) {
		this("app", id, amount);
	}

	public CounterReservation(String requesterId, CRDTIdentifier id, int amount) {
		this.requesterId = requesterId;
		this.resourceId = id;
		this.resource = amount;
	}

	@Override
	public void setClientTs(Timestamp ts) {
		this.ts = ts;
	}

	public CRDTIdentifier getResourceId() {
		return resourceId;
	}

	public Integer getResource() {
		return resource;
	}

	@Override
	public String getRequesterId() {
		return requesterId;
	}

	public String toString() {
		return "{" + resourceId + ", " + resource + ", " + requesterId + ", " + ts + "}";
	}
	public int hashCode() {
		return ts.hashCode();

	}

	public boolean equals(Object other) {
		return equals((CounterReservation) other);
	}

	private boolean equals(CounterReservation other) {
		return resourceId.equals(other.resourceId);
	}

	// ATTENTION: This assumes there is only two types of resources: locks and
	// counters
	@Override
	public int compareTo(ResourceRequest<Integer> other) {
		if (other instanceof CounterReservation) {
			int resouceComparison = resourceId.compareTo(((CounterReservation) other).resourceId);
			if (resouceComparison == 0) {
				return this.getResource() - other.getResource();
			} else {
				return resouceComparison;
			}
		} else {
			// CounterReservation has less priority than any other resource type
			return 1;
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

}
