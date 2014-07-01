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
import static sys.Context.Sys;

import java.net.UnknownHostException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.utils.Threading;
import umontreal.iro.lecuyer.stat.Tally;

public class RpcClient {
	public static Logger Log = Logger.getLogger(RpcClient.class.getName());

	public void doIt(String serverAddr) {

		Service stub = Networking.stub();

		final Endpoint server = Networking.resolve(RpcServer.URL);

		double T0 = Sys.currentTime();

		final SortedSet<Integer> values = new TreeSet<Integer>();

		final Tally rtt = new Tally("rtt");
		final Tally maxRTT = new Tally("max rtt");

		AtomicLong _total = new AtomicLong(0L);

		for (int n = 0;; n++) {
			synchronized (values) {
				values.add(n);
			}

			stub.asyncRequest(server, new Request(n), (Reply r) -> {
				_total.incrementAndGet();
				rtt.add(r.rtt());
				// // System.err.printf("%.1f/%.1f/%.1f - %.1f\n", rtt.min(),
				// // rtt.average(), rtt.max(), maxRTT.average());
				// if (rtt.numberObs() % 9999 == 0) {
				// maxRTT.add(rtt.max());
				// rtt.init();
				// }
					synchronized (values) {
						values.remove(r.val);
					}
				});

			long total = _total.get();
			if (total % 9999 == 0) {
				synchronized (values) {
					System.out.printf(stub.localEndpoint() + " #total %d, RPCs/sec %.1f Lag %d rpcs\n", total, total / (Sys.currentTime() - T0), (values.isEmpty() ? 0 : (n - values.first())));
				}
			}

			while (values.size() > 100)
				Threading.sleep(1);
		}
	}
	public static void main(String[] args) throws UnknownHostException {
		Log.setLevel(Level.ALL);

		String serverAddr = args.length > 0 ? args[0] : "localhost";

		new RpcClient().doIt(serverAddr);
	}
}
