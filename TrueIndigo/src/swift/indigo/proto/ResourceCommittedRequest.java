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

import java.util.HashSet;
import java.util.Set;

import swift.api.CRDTIdentifier;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.indigo.IndigoOperation;
import swift.indigo.ReservationsProtocolHandler;
import swift.indigo.ResourceManagerNode;
import swift.proto.CommitUpdatesRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class ResourceCommittedRequest extends IndigoOperation {

	private Timestamp clientTs;
	private transient boolean retry;
	private CommitUpdatesRequest updates;

	public ResourceCommittedRequest() {
		super();
	}

	public ResourceCommittedRequest(Timestamp clientTs, CommitUpdatesRequest commitUpdatesRequest) {
		super();
		this.clientTs = clientTs;
		this.updates = commitUpdatesRequest;
	}

	public ResourceCommittedRequest(String clientId, Timestamp clientTs) {
		super(clientId);
		this.clientTs = clientTs;
	}

	@Override
	public void deliverTo(Envelope conn, MessageHandler handler) {
		((ReservationsProtocolHandler) handler).onReceive(conn, this);

	}

	public Timestamp getClientTs() {
		return clientTs;
	}

	public String toString() {
		return String.format("Release: %s)", clientTs);
	}

	@Override
	public void deliverTo(ResourceManagerNode node) {
		node.process(this);
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof ResourceCommittedRequest) {
			return this.compareTo((ResourceCommittedRequest) other) == 0;
		} else
			return false;
	}

	// Release has the HighestPriority
	@Override
	public int compareTo(IndigoOperation o) {
		if (o instanceof ResourceCommittedRequest) {
			return clientTs.compareTo(((ResourceCommittedRequest) o).clientTs);
		} else
			return -1;
	}

	@Override
	public int hashCode() {
		return clientTs.hashCode();
	}

	public boolean isRetry() {
		return retry;
	}

	public void setRetry(boolean retry) {
		this.retry = retry;
	}

	public Set<CRDTIdentifier> getUpdatedCRDTs() {
		Set<CRDTIdentifier> ids = new HashSet<>();
		for (CRDTObjectUpdatesGroup<?> updatesList : updates.getObjectUpdateGroups()) {
			ids.add(updatesList.getTargetUID());
		}
		return ids;
	}

}
