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

import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTUpdate;

public class SharedLockUpdate implements CRDTUpdate<SharedLockCRDT> {

	private LockType type;
	private String ownerId;
	private String requesterId;
	private boolean isTransfer;
	private TripleTimestamp timestamp;

	public SharedLockUpdate() {
	}

	public SharedLockUpdate(String ownerId, String requesterId, LockType type, TripleTimestamp timestamp, boolean isTransfer) {
		this.type = type;
		this.ownerId = ownerId;
		this.timestamp = timestamp;
		this.requesterId = requesterId;
		this.isTransfer = isTransfer;
	}

	@Override
	public void applyTo(SharedLockCRDT crdt) {
		crdt.applySharedLockUpdate(this);
	}

	public LockType getType() {
		return type;
	}

	public TripleTimestamp getTimestamp() {
		return timestamp;
	}

	public String ownerId() {
		return ownerId == null ? "?" : ownerId;
	}

	public String requesterId() {
		return requesterId;
	}

	public boolean isTransfer() {
		return isTransfer;
	}

	public String toString() {
		return String.format("<%s, %s, %s, %s : %s>", type, ownerId, requesterId, timestamp, isTransfer ? "Transfer" : "Sharing");
	}
}
