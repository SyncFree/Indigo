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
package swift.indigo;

import swift.api.CRDTIdentifier;
import swift.clocks.Timestamp;
import swift.crdt.ShareableLock;

public class LockReservation implements ResourceRequest<ShareableLock> {

	public CRDTIdentifier resourceId;
	public ShareableLock type;
	public String requesterId;

	// ATTENTION: The timestamp is used in the hashcode but it might not be set.
	// The hashcode function should only be called when it is set (after the
	// lock is obtained)
	private Timestamp ts;

	LockReservation() {
	}

	public LockReservation(String requesterId, CRDTIdentifier resourceId, ShareableLock type) {
		this.requesterId = requesterId;
		this.resourceId = resourceId;
		this.type = type;
	}

	@Override
	public CRDTIdentifier getResourceId() {
		return resourceId;
	}

	@Override
	public ShareableLock getResource() {
		return type;
	}

	@Override
	public String getRequesterId() {
		return requesterId;
	}

	public String toString() {
		return "{ Requester: " + requesterId + " id: " + resourceId + ", type: " + type + "}";
	}

	public int hashCode() {
		return type.hashCode() ^ resourceId.hashCode() + ts.hashCode();
	}

	private boolean equals(LockReservation other) {
		return type.equals(other.type) && resourceId.equals(other.resourceId);
	}

	public boolean equals(Object other) {
		return other != null && equals((LockReservation) other);
	}

	@Override
	public int compareTo(ResourceRequest<ShareableLock> o) {
		if (o instanceof LockReservation) {
			LockReservation other = (LockReservation) o;
			int cmp = resourceId.compareTo(other.resourceId);
			return cmp != 0 ? cmp : type.ordinal() - other.type.ordinal();
		} else {
			// LockReservation has more priority than any other resource type
			return -1;
		}

	}

	@Override
	public void setClientTs(Timestamp ts) {
		this.ts = ts;
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
