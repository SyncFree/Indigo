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

/**
 * Timestamp given by the server to the client.
 * 
 * @author nmp
 */
public class GenerateTimestampReply {
	public enum GenerateStatus {
		/**
		 * Timestamp generated
		 */
		SUCCESSS,
		/**
		 * Already committed.
		 */
		ALREADY_COMMITTED,
		/**
		 * The transaction cannot be committed, because a given operation is
		 * invalid for some reason.
		 */
		INVALID_OPERATION
	}

	long cltClock;
	Timestamp timestamp;
	GenerateStatus status;

	public GenerateTimestampReply() {
	}

	public GenerateTimestampReply(final Timestamp timestamp, final long cltClock) {
		this.status = GenerateStatus.SUCCESSS;
		this.timestamp = timestamp;
		this.cltClock = cltClock;
	}

	public GenerateTimestampReply(final long cltClock) {
		this.status = GenerateStatus.ALREADY_COMMITTED;
		this.cltClock = cltClock;
	}

	/**
	 * @return timestamp that client can use, subject to renewal using keepalive
	 *         message
	 */
	public Timestamp getTimestamp() {
		return timestamp;
	}

	public GenerateStatus getStatus() {
		return status;
	}

	public long getCltClock() {
		return cltClock;
	}
}
