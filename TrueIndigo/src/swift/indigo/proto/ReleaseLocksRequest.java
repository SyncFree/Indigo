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
public class ReleaseLocksRequest extends ClientRequest {

	protected long serial;
	protected Timestamp cltTimestamp;

	/**
	 * Fake constructor for Kryo serialization.
	 */
	public ReleaseLocksRequest() {
	}

	public ReleaseLocksRequest(long serial, String clientId, Timestamp cltTimestamp) {
		super(clientId);
		this.serial = serial;
		this.cltTimestamp = cltTimestamp;
	}

	public long serial() {
		return serial;
	}

	public Timestamp cltTimestamp() {
		return cltTimestamp;
	}

	public String requesterId() {
		return super.getClientId();
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((IndigoProtocol) handler).onReceive(src, this);
	}

	public String toString() {
		return String.format("%s:%s)", requesterId(), cltTimestamp);
	}
}
