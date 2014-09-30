package swift.indigo;

import swift.api.CRDTIdentifier;
import swift.clocks.Timestamp;

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

	public String key() {
		StringBuilder sb = new StringBuilder();
		sb.append(resourceId.toString());
		return sb.toString();
	}

	@Override
	public Timestamp getClientTs() {
		return ts;
	}

}
