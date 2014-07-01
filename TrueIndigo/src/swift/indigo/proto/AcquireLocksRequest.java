/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.indigo.proto;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import swift.clocks.Timestamp;
import swift.crdt.LockType;
import swift.indigo.CounterReservation;
import swift.indigo.Lock;
import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Request to acquire a set of locks
 * 
 * @author smduarte
 */
public class AcquireLocksRequest extends ClientRequest implements Comparable<AcquireLocksRequest> {

	protected Lock[] locks;
	protected CounterReservation[] counters;
	protected Timestamp cltTimestamp;
	protected boolean isLocalRequest;

	/**
	 * Fake constructor for Kryo serialization.
	 */
	public AcquireLocksRequest() {
	}

	public AcquireLocksRequest(String clientId, Timestamp cltTimestamp, Lock[] locks, CounterReservation[] counters) {
		super(clientId);
		this.cltTimestamp = cltTimestamp;
		this.isLocalRequest = true;
		this.locks = locks;
		this.counters = counters;
	}

	public AcquireLocksRequest(String clientId, Timestamp cltTimestamp, Lock... locks) {
		super(clientId);
		this.cltTimestamp = cltTimestamp;
		this.isLocalRequest = true;
		this.locks = locks;
	}

	public AcquireLocksRequest(String clientId, Timestamp cltTimestamp, CounterReservation... counters) {
		super(clientId);
		this.cltTimestamp = cltTimestamp;
		this.isLocalRequest = true;
		this.counters = counters;
	}

	public AcquireLocksRequest(String serverId, Lock... locks) {
		super(serverId);
		this.isLocalRequest = false;
		this.locks = locks;
	}

	public AcquireLocksRequest(String serverId, Collection<Lock> locks) {
		super(serverId);
		this.isLocalRequest = false;
		this.locks = locks.toArray(new Lock[locks.size()]);
	}

	public boolean isLocalRequest() {
		return isLocalRequest;
	}

	public Timestamp cltTimestamp() {
		return cltTimestamp;
	}

	public Collection<Lock> locks() {
		if (this.locks != null) {
			return Arrays.asList(locks);
		} else {
			return new ArrayList<Lock>();
		}
	}

	public Collection<CounterReservation> counterReservations() {
		if (this.counters != null) {
			return Arrays.asList(counters);
		} else {
			return new ArrayList<CounterReservation>();
		}
	}

	public String requesterId() {
		return super.getClientId();
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((IndigoProtocol) handler).onReceive(src, this);
	}

	// Below is meant only for ordering remote pending requests...

	public int hashCode() {
		return Arrays.hashCode(locks) ^ Arrays.hashCode(counters) ^ requesterId().hashCode();
	}

	public boolean equals(Object other) {
		return other != null && equals((AcquireLocksRequest) other);
	}

	public boolean equals(AcquireLocksRequest other) {
		return requesterId().equals(other.requesterId()) && Arrays.equals(locks, other.locks) && Arrays.equals(counters, other.counters);
	}

	@Override
	public int compareTo(AcquireLocksRequest other) {
		int diff = locks[0].type.ordinal() - other.locks[0].type.ordinal();
		if (diff == 0)
			return requesterId().compareTo(other.requesterId());
		else
			return diff;
	}

	public static void main(String[] args) throws Exception {
		SortedSet<AcquireLocksRequest> set = new TreeSet<AcquireLocksRequest>();

		AcquireLocksRequest x1 = new AcquireLocksRequest("X", new Lock("xxx", LockType.EXCLUSIVE_ALLOW));
		AcquireLocksRequest x2 = new AcquireLocksRequest("X", new Lock("xxx", LockType.ALLOW));
		AcquireLocksRequest x3 = new AcquireLocksRequest("X", new Lock("xxx", LockType.FORBID));

		AcquireLocksRequest y1 = new AcquireLocksRequest("Y", new Lock("xxx", LockType.EXCLUSIVE_ALLOW));
		AcquireLocksRequest y2 = new AcquireLocksRequest("Y", new Lock("xxx", LockType.ALLOW));
		AcquireLocksRequest y3 = new AcquireLocksRequest("Y", new Lock("xxx", LockType.FORBID));

		set.add(x1);
		set.add(x2);
		set.add(x3);

		set.add(y1);
		set.add(y2);
		set.add(y3);

		set.add(x1);
		set.add(x2);
		set.add(x3);

		set.add(y1);
		set.add(y2);
		set.add(y3);

		System.err.println(set.size() + "--->" + set);
	}

	public String toString() {

		String locksAsString = "", countersAsString = "";
		if (locks != null) {
			locksAsString = Arrays.asList(locks).toString();
		}
		if (counters != null)
			countersAsString = Arrays.asList(counters).toString();

		return String.format("%s, %s : %s,%s,%s)", cltTimestamp, requesterId(), locksAsString, countersAsString, isLocalRequest() ? "Local" : " REMOTE ");
	}
}
