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
package swift.crdt;

import swift.crdt.core.CRDTUpdate;

public class BoundedCounterTransfer<T extends BoundedCounterCRDT<T>> implements CRDTUpdate<T> {

	private String originId, targetId;
	private int amount;

	public BoundedCounterTransfer() {

	}

	public BoundedCounterTransfer(String originId, String targetId, int amount) {
		this.originId = originId;
		this.targetId = targetId;
		this.amount = amount;
	}

	@Override
	public void applyTo(T crdt) {
		crdt.applyTransfer(this);

	}

	protected String getOriginId() {
		return originId;
	}

	protected void setOriginId(String originId) {
		this.originId = originId;
	}

	protected String getTargetId() {
		return targetId;
	}

	protected void setTargetId(String targetId) {
		this.targetId = targetId;
	}

	protected int getAmount() {
		return amount;
	}

	protected void setAmount(int amount) {
		this.amount = amount;
	}

	public String toString() {
		return String.format("Transfer %d from: %s to %s ", amount, originId, targetId);
	}
}
