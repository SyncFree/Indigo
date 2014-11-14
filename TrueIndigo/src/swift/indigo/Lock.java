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

import swift.crdt.LockType;

public class Lock {

    public String id;
    public LockType type;

    Lock() {
    }

    public Lock(String id, LockType type) {
        this.id = id;
        this.type = type;
    }

    public String id() {
        return id;
    }

    public LockType type() {
        return type;
    }

    public String toString() {
        return String.format("<%s : %s>", id, type);
    }

    public int hashCode() {
        return type.hashCode() ^ id.hashCode();
    }

    private boolean equals(Lock other) {
        return type.equals(other.type) && id.equals(other.id);
    }

    public boolean equals(Object other) {
        return other != null && equals((Lock) other);
    }
}
