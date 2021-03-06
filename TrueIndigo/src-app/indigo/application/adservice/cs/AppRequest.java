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
package indigo.application.adservice.cs;

import sys.net.api.Envelope;
import sys.net.api.Message;
import sys.net.api.MessageHandler;

public class AppRequest implements Message {

	int sessionId;
	String payload;

	AppRequest() {
	}

	public AppRequest(int sessionId, String payload) {
		this.payload = payload;
		this.sessionId = sessionId;
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((AppRequestHandler) handler).onReceive(src, this);
	}

}
