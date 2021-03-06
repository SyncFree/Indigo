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

import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.indigo.ReservationsProtocolHandler;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class IndigoCommitRequest extends swift.proto.CommitUpdatesRequest {

	long serial;
	boolean withLocks;

	public IndigoCommitRequest() {
		super();
	}

	public IndigoCommitRequest(long serial, String clientId, Timestamp cltTimestamp, CausalityClock dependencyClock, List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups, boolean withLocks) {
		super(clientId, cltTimestamp, dependencyClock, objectUpdateGroups);
		this.serial = serial;
		this.withLocks = withLocks;
	}

	public long serial() {
		return serial;
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((ReservationsProtocolHandler) handler).onReceive(src, this);
	}

	public boolean withLocks() {
		return withLocks;
	}
}
