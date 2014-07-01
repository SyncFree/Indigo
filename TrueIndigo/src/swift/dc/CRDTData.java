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
package swift.dc;

import swift.api.CRDT;
import swift.crdt.core.ManagedCRDT;

public class CRDTData<V extends CRDT<V>> extends ManagedCRDT<V> {

	CRDTData(ManagedCRDT<V> crdt) {
		super(crdt);
	}

	public void pruneIfPossible() {
		// TODO Auto-generated method stub

	}

	// /**
	// * true if the entry corresponds to an object that does not exist
	// */
	// boolean empty;
	// /**
	// * crdt object
	// */
	// ManagedCRDT<V> crdt;
	// /**
	// * crdt object
	// */
	// // CRDT<V> prunedCrdt;
	// /**
	// * CRDT unique identifier
	// */
	// CRDTIdentifier id;
	// /**
	// * current clock reflects all updates and their causal past
	// */
	// // FIXME(Marek): why do we need a direct reference to the clock?
	// // Arent't WrappedCRDT methods good enough?
	// CausalityClock clock;
	// /**
	// * current clock reflects all updates and their causal past, from the
	// * perspective of clients
	// */
	// // CausalityClock cltClock;
	// /**
	// * prune clock reflects the updates that have been discarded, making it
	// * impossible to access a snapshot that is dominated by this clock
	// */
	// // FIXME(Marek): why do we need a direct reference to the pruneClock?
	// // Arent't WrappedCRDT methods good enough?
	// CausalityClock pruneClock;
	// transient long lastPrunedTime;
	// transient CausalityClock lastPrunedClock;
	// transient Object dbInfo;
	//
	// CRDTData() {
	// lastPrunedTime = -1;
	// }
	//
	// CRDTData(CRDTIdentifier id) {
	// lastPrunedTime = -1;
	// this.id = id;
	// this.empty = true;
	// }
	//
	// boolean pruneIfPossible() {
	// long curTime = System.currentTimeMillis();
	// if (lastPrunedTime == -1) {
	// lastPrunedTime = curTime;
	// lastPrunedClock = (CausalityClock) clock.copy();
	// lastPrunedClock.trim();
	// }
	// if (lastPrunedTime + DCConstants.PRUNING_INTERVAL < curTime) {
	// crdt.prune(lastPrunedClock, false);
	// pruneClock = lastPrunedClock;
	// lastPrunedTime = curTime;
	// lastPrunedClock = (CausalityClock) clock.copy();
	// lastPrunedClock.trim();
	// return true;
	// } else
	// return false;
	//
	// }
	//

	//
	// public int hashCode() {
	// return id.hashCode();
	// }
	//
	// public boolean equals(Object obj) {
	// return obj instanceof CRDTData && id.equals(((CRDTData) obj).id);
	//
	// }
	//
	// public Object getDbInfo() {
	// return dbInfo;
	// }
	//
	// public void setDbInfo(Object dbInfo) {
	// this.dbInfo = dbInfo;
	// }
	//
	// public void mergeInto(CRDTData<?> d) {
	// // FIXME: (Marek) It used to be "true", but I have a sense it was
	// // incorrect.
	// empty &= d.empty;
	// crdt.merge((ManagedCRDT<V>) d.crdt);
	// clock.merge(d.clock);
	// // if( DCDataServer.prune) {
	// // this.prunedCrdt.merge((CRDT<V>)d.crdt);
	// // }
	// pruneClock.merge(d.pruneClock);
	// // cltClock.merge(d.cltClock);
	// }
	//
	// public boolean isEmpty() {
	// return empty;
	// }
	//
	// public ManagedCRDT<V> getCrdt() {
	// return crdt;
	// }
	//
	// public CRDTIdentifier getId() {
	// return id;
	// }
	//
	// public CausalityClock getClock() {
	// return clock;
	// }
	//
	// public CausalityClock getPruneClock() {
	// return pruneClock;
	// }
}
