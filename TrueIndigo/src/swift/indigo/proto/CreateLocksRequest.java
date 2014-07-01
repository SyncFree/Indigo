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

import java.util.Arrays;
import java.util.Collection;

import swift.indigo.CounterReservation;
import swift.indigo.Lock;
import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Request to acquire a set of locks
 * 
 * @author smduarte
 */
public class CreateLocksRequest extends ClientRequest {

	protected Lock[] locks;
	protected CounterReservation[] counters;

	/**
	 * Fake constructor for Kryo serialization.
	 */
	public CreateLocksRequest() {
	}

	public CreateLocksRequest(String clientId, Lock... locks) {
		super(clientId);
		this.locks = locks;
	}

	public CreateLocksRequest(String clientId, Lock[] locks, CounterReservation[] counters) {
		super(clientId);
		this.locks = locks;
		this.counters = counters;
	}

	public CreateLocksRequest(String serverId, Collection<Lock> locks) {
		super(serverId);
		this.locks = locks.toArray(new Lock[locks.size()]);
	}

	public CreateLocksRequest(String serverId, Collection<Lock> locks, Collection<CounterReservation> counters) {
		super(serverId);
		this.locks = locks.toArray(new Lock[locks.size()]);
		this.counters = counters.toArray(new CounterReservation[counters.size()]);
	}

	public boolean hasLocks() {
		if (locks != null) {
			return true;
		} else {
			return false;
		}

	}

	public Collection<Lock> locks() {
		return Arrays.asList(locks);
	}

	public boolean hasCounters() {
		if (counters != null) {
			return true;
		} else {
			return false;
		}

	}

	public Collection<CounterReservation> counters() {
		return Arrays.asList(counters);
	}

	public String requesterId() {
		return super.getClientId();
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((IndigoProtocol) handler).onReceive(src, this);
	}

	public String toString() {
		return String.format("%s, %s)", requesterId(), Arrays.asList(locks));
	}
}
