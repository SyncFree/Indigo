package swift.indigo.proto;

import java.util.Collection;
import java.util.Iterator;
import java.util.PriorityQueue;

import swift.clocks.Timestamp;
import swift.indigo.IndigoOperation;
import swift.indigo.IndigoResourceManager;
import swift.indigo.ReservationsProtocolHandler;
import swift.indigo.ResourceManagerNode;
import swift.indigo.ResourceRequest;
import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;
import sys.utils.Threading;

public class AcquireResourcesRequest extends ClientRequest implements IndigoOperation {

	Timestamp clientTs;
	PriorityQueue<ResourceRequest<?>> requests;
	private transient Envelope handler;

	public AcquireResourcesRequest() {

	}

	public AcquireResourcesRequest(String clientId, Timestamp cltTimestamp, Collection<ResourceRequest<?>> resources) {
		super(clientId);
		this.clientTs = cltTimestamp;
		this.requests = new PriorityQueue<>(resources);
	}

	public AcquireResourcesRequest(AcquireResourcesRequest other) {
		super(other.clientId);
		this.clientTs = other.clientTs;
		this.requests = new PriorityQueue<>(other.requests);
	}

	@Override
	public void deliverTo(Envelope handle, MessageHandler handler) {
		((ReservationsProtocolHandler) handler).onReceive(handle, this);
	}

	public Timestamp getClientTs() {
		return clientTs;
	}

	public Collection<ResourceRequest<?>> getRequests() {
		return requests;
	}

	public int hashCode() {
		return requests.hashCode() ^ getClientId().hashCode();
	}

	// public boolean equals(AcquireResourcesRequest other) {
	// // FIXME: Only compares that the client timestamp is the same? -- Seems
	// // to be working
	// return getClientId().equals(other.getClientId())/*
	// * &&
	// * requests.equals(other
	// * .requests)
	// */;
	// }

	@Override
	public boolean equals(Object other) {
		if (other instanceof AcquireResourcesRequest) {
			return this.compareTo((AcquireResourcesRequest) other) == 0;
		} else
			return false;
	}

	// TODO: Must test more the combination of locks and counters
	@Override
	public int compareTo(IndigoOperation o) {
		if (o instanceof AcquireResourcesRequest) {
			AcquireResourcesRequest other = (AcquireResourcesRequest) o;
			Iterator<ResourceRequest<?>> it = requests.iterator();
			Iterator<ResourceRequest<?>> otherIt = other.requests.iterator();
			int diff = 0;
			while (diff == 0) {
				if (it.hasNext()) {
					ResourceRequest myElem = it.next();
					if (otherIt.hasNext()) {
						ResourceRequest otherElem = otherIt.next();
						diff = myElem.compareTo(otherElem);
					} else {
						return 1;
					}
				} else {
					// If the resources request list is smaller it is first in
					// the lexicographical order
					if (otherIt.hasNext())
						return -1;
					else {
						break;
					}
				}
			}
			if (diff == 0) {
				int clientComparison = getClientId().compareTo(other.getClientId());
				if (clientComparison == 0) {
					return clientTs.compareTo(other.clientTs);
				} else
					return clientComparison;
			} else
				return diff;
		} else
			return Integer.MAX_VALUE;
	}
	public String toString() {
		String resourcesAsString = requests.toString();
		return String.format("Acquire: %s, %s : %s)", clientTs, getClientId(), resourcesAsString);
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
	public void deliverTo(ResourceManagerNode node) {
		node.processWithReply(handler, this);
	}

	public void setHandler(Envelope conn) {
		this.handler = conn;

	}

}

// protected LockReservation[] locks;
// protected CounterReservation[] counters;
// protected Timestamp cltTimestamp;
// protected boolean isLocalRequest;
//
// /**
// * Fake constructor for Kryo serialization.
// */
// public AcquireLocksRequest() {
// }
//
// public AcquireLocksRequest(String clientId, Timestamp cltTimestamp,
// LockReservation[] locks, CounterReservation[] counters) {
// super(clientId);
// this.locks = locks = sortedLocks(locks);
// this.counters = sortedCounters(counters);
// this.isLocalRequest = true;
// this.cltTimestamp = cltTimestamp;
// }
//
// public AcquireLocksRequest(String clientId, Timestamp cltTimestamp,
// LockReservation... locks) {
// super(clientId);
// this.locks = sortedLocks(locks);
// this.counters = new CounterReservation[0];
// this.isLocalRequest = true;
// this.cltTimestamp = cltTimestamp;
// }
//
// public AcquireLocksRequest(String clientId, Timestamp cltTimestamp,
// CounterReservation... counters) {
// super(clientId);
// this.locks = new LockReservation[0];
// this.counters = sortedCounters(counters);
// this.isLocalRequest = true;
// this.cltTimestamp = cltTimestamp;
// }
//
// public AcquireLocksRequest(String serverId, LockReservation... locks) {
// super(serverId);
// this.locks = sortedLocks(locks);
// this.isLocalRequest = false;
// }
//
// public AcquireLocksRequest(String serverId, Collection<LockReservation>
// locks) {
// super(serverId);
// this.isLocalRequest = false;
// this.locks = sortedLocks(locks);
// }
//
// public boolean isLocalRequest() {
// return isLocalRequest;
// }
//
// public Timestamp cltTimestamp() {
// return cltTimestamp;
// }
//
// public Collection<LockReservation> locks() {
// if (this.locks != null) {
// return Arrays.asList(locks);
// } else {
// return new ArrayList<LockReservation>();
// }
// }
//
// public Collection<CounterReservation> counterReservations() {
// if (this.counters != null) {
// return Arrays.asList(counters);
// } else {
// return new ArrayList<CounterReservation>();
// }
// }
//
// public String requesterId() {
// return super.getClientId();
// }
//
// @Override
// public void deliverTo(RpcHandle conn, RpcHandler handler) {
// ((IndigoProtocolHandler) handler).onReceive(conn, this);
// }
//
// // Below is meant only for ordering remote pending requests...
//
// public int hashCode() {
// return Arrays.hashCode(locks) ^ Arrays.hashCode(counters) ^
// requesterId().hashCode();
// }
//
// public boolean equals(Object other) {
// return other != null && equals((AcquireLocksRequest) other);
// }
//
// public boolean equals(AcquireLocksRequest other) {
// return requesterId().equals(other.requesterId()) && Arrays.equals(locks,
// other.locks)
// && Arrays.equals(counters, other.counters);
// }
//
// @Override
// public int compareTo(AcquireLocksRequest other) {
// int diff = locks[0].type.ordinal() - other.locks[0].type.ordinal();
// if (diff == 0)
// return requesterId().compareTo(other.requesterId());
// else
// return diff;
// }
//
// // public static void main(String[] args) throws Exception {
// // SortedSet<AcquireLocksRequest> set = new TreeSet<AcquireLocksRequest>();
// //
// // AcquireLocksRequest x1 = new AcquireLocksRequest("X", new Lock("xxx",
// // LockType.EXCLUSIVE_ALLOW));
// // AcquireLocksRequest x2 = new AcquireLocksRequest("X", new Lock("xxx",
// // LockType.ALLOW));
// // AcquireLocksRequest x3 = new AcquireLocksRequest("X", new Lock("xxx",
// // LockType.FORBID));
// //
// // AcquireLocksRequest y1 = new AcquireLocksRequest("Y", new Lock("xxx",
// // LockType.EXCLUSIVE_ALLOW));
// // AcquireLocksRequest y2 = new AcquireLocksRequest("Y", new Lock("xxx",
// // LockType.ALLOW));
// // AcquireLocksRequest y3 = new AcquireLocksRequest("Y", new Lock("xxx",
// // LockType.FORBID));
// //
// // set.add(x1);
// // set.add(x2);
// // set.add(x3);
// //
// // set.add(y1);
// // set.add(y2);
// // set.add(y3);
// //
// // set.add(x1);
// // set.add(x2);
// // set.add(x3);
// //
// // set.add(y1);
// // set.add(y2);
// // set.add(y3);
// //
// // System.err.println(set.size() + "--->" + set);
// // }
//

