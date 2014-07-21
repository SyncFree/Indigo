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

import swift.api.CRDTIdentifier;
import swift.indigo.ReservationsProtocolHandler;
import swift.proto.ClientRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

/**
 * Client request to fetch a particular version of an object.
 * 
 * @author smduarte
 */
public class FetchObjectRequest extends ClientRequest {

    protected CRDTIdentifier uid;
    protected boolean subscribe;

    public FetchObjectRequest() {
    }

    public FetchObjectRequest(String clientId, CRDTIdentifier uid, boolean subscribe) {
        super(clientId);
        this.uid = uid;
        this.subscribe = subscribe;
    }

    public boolean hasSubscription() {
        return subscribe;
    }

    /**
     * @return id of the requested object
     */
    public CRDTIdentifier getUid() {
        return uid;
    }

    @Override
    public void deliverTo(Envelope src, MessageHandler handler) {
        ((ReservationsProtocolHandler) handler).onReceive(src, this);
    }
}
