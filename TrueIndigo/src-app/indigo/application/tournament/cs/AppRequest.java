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
package indigo.application.tournament.cs;

import sys.net.api.rpc.RpcHandle;
import sys.net.api.rpc.RpcHandler;
import sys.net.api.rpc.RpcMessage;

public class AppRequest implements RpcMessage {

    int sessionId;
    String payload;

    AppRequest() {
    }

    public AppRequest(int sessionId, String payload) {
        this.payload = payload;
        this.sessionId = sessionId;
    }

    @Override
    public void deliverTo(RpcHandle handle, RpcHandler handler) {
        if (handle.expectingReply())
            ((AppRequestHandler) handler).onReceive(handle, this);
        else
            ((AppRequestHandler) handler).onReceive(this);
    }

}
