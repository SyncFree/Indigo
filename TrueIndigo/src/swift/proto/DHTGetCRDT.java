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
package swift.proto;

import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import sys.net.api.Envelope;
import sys.net.api.Message;
import sys.net.api.MessageHandler;

/**
 * Object for getting a crdt
 * 
 * @author preguica, smduarte
 */
final public class DHTGetCRDT implements Message {

	String clientId;
	CRDTIdentifier id;
	CausalityClock version;
	boolean subscribeUpdates;

	public DHTGetCRDT() {
	}

	public DHTGetCRDT(CRDTIdentifier id, CausalityClock version, String clientId, boolean subscribeUpdates) {
		super();
		this.id = id;
		this.version = version;
		this.clientId = clientId;
		this.subscribeUpdates = subscribeUpdates;
	}

	public String getCltId() {
		return clientId;
	}

	public CRDTIdentifier getId() {
		return id;
	}

	public CausalityClock getVersion() {
		return version;
	}

	public boolean subscribesUpdates() {
		return subscribeUpdates;
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((DataServerProtocol) handler).onReceive(src, this);
	}
}
