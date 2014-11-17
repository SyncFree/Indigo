/**
-------------------------------------------------------------------

Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

-------------------------------------------------------------------
**/
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
package swift.crdt;

import java.util.Map;

import swift.api.CRDTIdentifier;
import swift.api.TxnHandle;
import swift.clocks.CausalityClock;
import sys.KryoLib;

public class LowerBoundCounterCRDT extends BoundedCounterCRDT<LowerBoundCounterCRDT> {

	// For kryo
	public LowerBoundCounterCRDT() {
	}

	public LowerBoundCounterCRDT(CRDTIdentifier id) {
		this(id, 0);
	}

	public LowerBoundCounterCRDT(CRDTIdentifier id, int initVal) {
		super(id, initVal);
	}

	public LowerBoundCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, int initVal,
			Map<String, Map<String, Integer>> permissions, Map<String, Integer> decrements) {
		super(id, txn, clock, initVal, permissions, decrements);
	}

	@Override
	public LowerBoundCounterCRDT copy() {
		return KryoLib.copy(this);
		// Map<String, Map<String, Integer>> permCopy = new HashMap<String,
		// Map<String, Integer>>(permissions);
		// for (Entry<String, Map<String, Integer>> entry :
		// permissions.entrySet()) {
		// permCopy.put(entry.getKey(), new HashMap<String,
		// Integer>(entry.getValue()));
		// }
		// return new LowerBoundCounterCRDT(id, getTxnHandle(), getClock(),
		// initVal, permCopy,
		// new HashMap<String, Integer>(delta));
	}

	public boolean increment(int amount, String siteId) {
		BoundedCounterIncrement<LowerBoundCounterCRDT> update = new BoundedCounterIncrement<LowerBoundCounterCRDT>(
				siteId, amount);
		applyInc(update);
		registerLocalOperation(update);
		return true;
	}

	public boolean decrement(int amount, String siteId) {
		if (availableSiteId(siteId) >= amount) {
			BoundedCounterDecrement<LowerBoundCounterCRDT> update = new BoundedCounterDecrement<LowerBoundCounterCRDT>(
					siteId, amount);
			applyDec(update);
			registerLocalOperation(update);
			return true;
		}
		return false;
	}

	public boolean transfer(int amount, String originId, String targetId) {
		if (availableSiteId(originId) >= amount) {
			BoundedCounterTransfer<LowerBoundCounterCRDT> update = new BoundedCounterTransfer<LowerBoundCounterCRDT>(
					originId, targetId, amount);
			applyTransfer(update);
			registerLocalOperation(update);
			return true;
		}
		return false;
	}

	protected void applyDec(BoundedCounterDecrement<LowerBoundCounterCRDT> decUpdate) {
		checkExistsPermissionPair(decUpdate.getSiteId(), decUpdate.getSiteId());

		// TODO: WHAT????
		int current = delta.get(decUpdate.getSiteId());

		delta.put(decUpdate.getSiteId(), current + decUpdate.getAmount());
		val -= decUpdate.getAmount();

	}

	protected void applyInc(BoundedCounterIncrement<LowerBoundCounterCRDT> incUpdate) {
		checkExistsPermissionPair(incUpdate.getSiteId(), incUpdate.getSiteId());
		Map<String, Integer> sitePermissions = permissions.get(incUpdate.getSiteId());
		sitePermissions.put(incUpdate.getSiteId(), sitePermissions.get(incUpdate.getSiteId()) + incUpdate.getAmount());
		val += incUpdate.getAmount();
	}

	public void setTxnHandle(TxnHandle handle) {
		super.txn = handle;
	}

	public String toString() {
		return "PER " + permissions + " DEC " + delta;
	}

}
