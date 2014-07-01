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
package bench;

import static sys.Context.Networking;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Threading;

public class RpcServer implements ProbeHandler {

	final static String URL = "tcp://*:9999";

	Service srv;

	RpcServer() {
		srv = Networking.bind(Networking.resolve(URL), this);
		System.out.println("Server ready...");
	}

	@Override
	public void onReceive(final Envelope e, final Request req) {
		e.reply(new Reply(req.val, req.timestamp));
	}

	public static void main(final String[] args) {
		new RpcServer();
		Threading.sleep(10000000);
	}
}
