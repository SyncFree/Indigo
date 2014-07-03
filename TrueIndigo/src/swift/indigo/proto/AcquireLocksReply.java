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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.indigo.CounterReservation;
import swift.indigo.Lock;

/**
 * Server reply to client's latest known clock request.
 * 
 * @author smduarte
 */
public class AcquireLocksReply {
	long serial;

	protected boolean succeded;
	protected Timestamp timestamp;
	protected CausalityClock snapshot;
	protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;

	transient Collection<CounterReservation> counters;
	transient Collection<Lock> locks;
	transient Timestamp cltTimestamp;

	/**
	 * Fake constructor for Kryo serialization. Do NOT use.
	 */
	public AcquireLocksReply() {
	}

	public AcquireLocksReply(boolean dummy) {
		this.succeded = false;
		if (dummy != false)
			throw new RuntimeException("Expected false...");
	}

	public AcquireLocksReply(Timestamp cltTimestamp, Timestamp timestamp, CausalityClock snapshot, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups, Collection<Lock> locks, Collection<CounterReservation> counters) {
		this.succeded = true;
		this.snapshot = snapshot;
		this.timestamp = timestamp;
		this.cltTimestamp = cltTimestamp;
		this.objectUpdateGroups = objectUpdateGroups;
		this.locks = locks;
		this.counters = counters;
	}

	public AcquireLocksReply(Timestamp cltTimestamp, Timestamp timestamp, CausalityClock snapshot, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups, Collection<Lock> locks) {
		this.succeded = true;
		this.snapshot = snapshot;
		this.timestamp = timestamp;
		this.objectUpdateGroups = objectUpdateGroups;
		this.locks = locks;
		this.counters = new LinkedList<CounterReservation>();
	}

	// Used for weak consistency emulation...
	public AcquireLocksReply(long serial, CausalityClock currentClockEstimate) {
		this.serial = serial;
		this.succeded = true;
		this.snapshot = currentClockEstimate;
		this.timestamp = null;
		this.objectUpdateGroups = Collections.emptyList();
	}

	public boolean matches(Timestamp clTimestamp) {
		return this.cltTimestamp.equals(cltTimestamp);
	}

	public AcquireLocksReply setSerial(long serial) {
		this.serial = serial;
		return this;
	}

	public long serial() {
		return serial;
	}

	public Collection<Lock> locks() {
		return locks;
	}

	public Collection<CounterReservation> counters() {
		return counters;
	}

	public Timestamp timestamp() {
		return timestamp;
	}

	public boolean acquiredLocksAndCounters() {
		return succeded;
	}

	public List<CRDTObjectUpdatesGroup<?>> lockOps() {
		return objectUpdateGroups;
	}

	/**
	 * @return snapshot point when locks were acquired...
	 */
	public CausalityClock getSnapshot() {
		return snapshot;
	}
}
