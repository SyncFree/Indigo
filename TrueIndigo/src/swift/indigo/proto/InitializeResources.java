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
import swift.indigo.LockReservation;
import swift.indigo.ReservationsProtocolHandler;
import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Request to acquire a set of locks
 * 
 * @author smduarte
 */
public class InitializeResources extends ClientRequest {

    protected LockReservation[] locks;
    protected CounterReservation[] counters;

    /**
     * Fake constructor for Kryo serialization.
     */
    public InitializeResources() {
    }

    public InitializeResources(String clientId, LockReservation... locks) {
        super(clientId);
        this.locks = locks;
    }

    public InitializeResources(String clientId, LockReservation[] locks, CounterReservation[] counters) {
        super(clientId);
        this.locks = locks;
        this.counters = counters;
    }

    public InitializeResources(String serverId, Collection<LockReservation> locks) {
        super(serverId);
        this.locks = locks.toArray(new LockReservation[locks.size()]);
    }

    public InitializeResources(String serverId, Collection<LockReservation> locks,
            Collection<CounterReservation> counters) {
        super(serverId);
        this.locks = locks.toArray(new LockReservation[locks.size()]);
        this.counters = counters.toArray(new CounterReservation[counters.size()]);
    }

    public boolean hasLocks() {
        if (locks != null) {
            return true;
        } else {
            return false;
        }

    }

    public Collection<LockReservation> locks() {
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
    public void deliverTo(Envelope conn, MessageHandler handler) {
        ((ReservationsProtocolHandler) handler).onReceive(conn, this);
    }

    public String toString() {
        return String.format("%s, %s)", requesterId(), Arrays.asList(locks));
    }
}
