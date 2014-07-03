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

import swift.clocks.Timestamp;
import sys.net.api.Envelope;
import sys.net.api.Message;
import sys.net.api.MessageHandler;

/**
 * Informs the Sequencer Server that the given timestamp should be
 * committed/rollbacked.
 * 
 * @author preguica, smduarte
 */
public class CommitTimestampRequest implements Message {
	Timestamp timestamp;
	Timestamp cltTimestamp;

	public long rtClock;

	transient Envelope source;

	public CommitTimestampRequest() {
	}

	public CommitTimestampRequest(Timestamp timestamp, Timestamp cltTimestamp) {
		this.timestamp = timestamp;
		this.cltTimestamp = cltTimestamp;
	}

	public long latency() {
		return System.currentTimeMillis() - rtClock;
	}

	public void setSource(Envelope source) {
		this.source = source;
	}

	public Envelope getSource() {
		return this.source;
	}

	/**
	 * @return the timestamp previously received from the server
	 */
	public Timestamp getTimestamp() {
		return timestamp;
	}

	/**
	 * @return the timestamp of the client
	 */
	public Timestamp getCltTimestamp() {
		return cltTimestamp;
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((SequencerProtocol) handler).onReceive(src, this);
	}

	public String toString() {
		return Long.toString(timestamp.getCounter());
	}
}
