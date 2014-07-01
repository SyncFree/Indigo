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
package swift.pubsub;

import swift.proto.ClientRequest;
import swift.proto.SurrogateProtocol;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Scout request to update its subscriptions.
 * 
 * @author smduarte
 */
public class PubSubHandshake extends ClientRequest {

	/**
	 * For Kryo, do NOT use.
	 */
	PubSubHandshake() {
	}

	public PubSubHandshake(String clientId) {
		super(clientId);
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((SurrogateProtocol) handler).onReceive(src, this);
	}
}
