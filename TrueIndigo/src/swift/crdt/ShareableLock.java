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

public enum ShareableLock {
	EXCLUSIVE_ALLOW, FORBID, ALLOW, NONE;

	public boolean isShareable() {
		return !this.equals(EXCLUSIVE_ALLOW);
	}

	public boolean isExclusive() {
		return this.equals(EXCLUSIVE_ALLOW);
	}

	public boolean isCompatible(ShareableLock resource) {
		return this.equals(resource) && !this.equals(EXCLUSIVE_ALLOW);
	}

	public static ShareableLock getDefault() {
		return ALLOW;
	}

}
