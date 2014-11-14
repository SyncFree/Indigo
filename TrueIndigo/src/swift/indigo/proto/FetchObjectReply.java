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
package swift.indigo.proto;

import swift.crdt.core.ManagedCRDT;

/**
 * Server reply to object version fetch request.
 * 
 * @author smduarte
 */
public class FetchObjectReply {

	public enum FetchStatus {
		/**
		 * The reply contains requested version.
		 */
		OK,
		/**
		 * The requested object is not in the store.
		 */
		OBJECT_NOT_FOUND,
	}

	protected FetchStatus status;
	protected ManagedCRDT<?> crdt;

	public FetchObjectReply() {
	}

	public FetchObjectReply(ManagedCRDT<?> crdt) {
		this.crdt = crdt;
		this.status = crdt == null ? FetchStatus.OBJECT_NOT_FOUND : FetchStatus.OK;
	}

	/**
	 * @return status code of the reply
	 */
	public FetchStatus getStatus() {
		return status;
	}

	/**
	 * @return state of an object requested by the client; null if
	 *         {@link #getStatus()} is {@link FetchStatus#OBJECT_NOT_FOUND}.
	 */

	public ManagedCRDT<?> getCrdt() {
		return crdt;
	}
}
