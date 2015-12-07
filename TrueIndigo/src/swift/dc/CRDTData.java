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
import swift.clocks.CausalityClock;
import swift.crdt.core.ManagedCRDT;

public class CRDTData<V extends CRDT<V>> extends ManagedCRDT<V> {

	long lastPrunedTime = 0L;

	CRDTData(ManagedCRDT<V> crdt) {
		super(crdt);
	}

	boolean pruneIfPossible(CausalityClock clock) {
		// System.exit(0);
		long now = System.currentTimeMillis();
		if (lastPrunedTime + Defaults.PRUNING_INTERVAL < now) {
			super.prune(clock, false);
			lastPrunedTime = now;
			return true;
		} else
			return false;
	}
}
