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

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import sys.net.api.Envelope;
import sys.net.api.Message;
import sys.net.api.MessageHandler;

/**
 * Object for executing operations in a crdt
 * 
 * @author preguica
 */
public class DHTExecCRDT implements Message {

	CRDTObjectUpdatesGroup<?> grp;
	CausalityClock curDCVersion;
	CausalityClock snapshotVersion;
	CausalityClock trxVersion;
	Timestamp cltTs;

	public DHTExecCRDT() {
	}

	public DHTExecCRDT(CRDTObjectUpdatesGroup<?> grp, Timestamp cltTs, Timestamp prvCltTs, CausalityClock curDCVersion) {
		this.grp = grp;
		this.cltTs = cltTs;
		this.curDCVersion = curDCVersion;
	}

	public CRDTObjectUpdatesGroup<?> getGrp() {
		return grp;
	}

	public CausalityClock getSnapshotVersion() {
		return snapshotVersion;
	}

	public CausalityClock getTrxVersion() {
		return trxVersion;
	}

	public Timestamp getTxTs() {
		return grp.getTimestamps().get(0);
	}

	public Timestamp getCltTs() {
		return cltTs;
	}

	public CausalityClock getCurrentState() {
		return curDCVersion;
	}

	@Override
	public void deliverTo(Envelope e, MessageHandler handler) {
		((DataServerProtocol) handler).onReceive(e, this);
	}
}
