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

import swift.clocks.Timestamp;
import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Request to release the locks associated with a timestamp...
 * 
 * @author smduarte
 */
public class DiscardSnapshotRequest extends ClientRequest {

	protected Timestamp cltTimestamp;

	/**
	 * Fake constructor for Kryo serialization.
	 */
	public DiscardSnapshotRequest() {
	}

	public DiscardSnapshotRequest(String clientId, Timestamp cltTimestamp) {
		super(clientId);
		this.cltTimestamp = cltTimestamp;
	}

	public Timestamp cltTimestamp() {
		return cltTimestamp;
	}

	public String requesterId() {
		return super.getClientId();
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((IndigoProtocolHandler) handler).onReceive(src, this);
	}

	// Below is meant only for ordering remote pending requests...

	public String toString() {
		return String.format("%s:%s)", requesterId(), cltTimestamp);
	}
}
