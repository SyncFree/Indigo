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
package indigo.application.adservice;

import java.io.PrintStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import swift.indigo.Indigo;
import sys.utils.Progress;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Executing IndigoAdService operations.
 */
public class AdServiceApp {
	private static Logger Log = Logger.getLogger(AdServiceApp.class.getName());

	protected int thinkTime;
	protected int randomOps;

	protected PrintStream bufferedOutput;

	private Properties props;
	protected AtomicInteger commandsDone = new AtomicInteger(0);
	protected AtomicInteger totalCommands = new AtomicInteger(0);

	int numAds;
	int numCopies;
	int maxViewCountAd;
	int maxViewCountAdCopy;
	boolean onlyGlobal;

	public List<String> populateWorkloadFromConfig() {

		bufferedOutput = new PrintStream(System.out, false);

		props = Props.parseFile("indigo-adservice", bufferedOutput, "indigo-adservice-test.props");

		numAds = Props.intValue(props, "adservice.numAds", 5);
		numCopies = Props.intValue(props, "adservice.numCopies", 100);
		maxViewCountAd = Props.intValue(props, "adservice.maxViewCountAd", 1000);
		maxViewCountAdCopy = Props.intValue(props, "adservice.maxViewCountAdCopy", 1000);
		randomOps = Props.intValue(props, "adservice.randomOps", 1);
		thinkTime = Props.intValue(props, "adservice.thinkTime", 1000);
		onlyGlobal = Props.boolValue(props, "adservice.onlyGlobal", true);

		return Workload.populate(numAds, numCopies, maxViewCountAd, maxViewCountAdCopy);
	}

	public Workload getWorkloadFromConfig(int site, int numberOfSites) {
		if (props == null)
			populateWorkloadFromConfig();

		return Workload.doMixed(site, randomOps, numberOfSites);
	}

	void runClientSession(AdServiceOps serviceClient, final int sessionId, final Workload commands, boolean loop4Ever) {

		totalCommands.addAndGet(commands.size());
		final long sessionStartTime = System.currentTimeMillis();
		final String initSessionLog = String.format("%d,%s,%d,%d", -1, "INIT", 0, sessionStartTime);
		bufferedOutput.println(initSessionLog);
		if (sessionId == 0)
			bufferedOutput.println("; sessionId,responseCode,copyValue,globalValue,execTime,endTime");
		do
			for (String cmdLine : commands) {
				try {
					long txnStartTime = System.currentTimeMillis();
					Results res = runCommandLine(sessionId, serviceClient, cmdLine);
					res.setStartTime(txnStartTime).setSession(sessionId).logTo(bufferedOutput);
					Threading.sleep(thinkTime);
					commandsDone.incrementAndGet();
				} catch (Exception x) {
					x.printStackTrace();
				}
			}
		while (loop4Ever);

		final long now = System.currentTimeMillis();
		final long sessionExecTime = now - sessionStartTime;
		bufferedOutput.println(String.format("%d,%s,%d,%d", sessionId, "TOTAL", sessionExecTime, now));
		bufferedOutput.flush();
	}

	public Results runCommandLine(int sessionId, AdServiceOps adServiceClient, String cmdLine) {
		String[] toks = cmdLine.split(";");
		final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
		Results result = null;
		switch (cmd) {
			case VIEW_AD :
				if (toks.length == 3) {
					result = adServiceClient.viewAd(Integer.parseInt(toks[1]), Integer.parseInt(toks[2]), onlyGlobal);
					break;
				}
			default :
				Log.warning("Can't parse command line :" + cmdLine);
				Log.warning("Exiting...");
				System.exit(1);
		}
		return result;
	}

	String progressMsg = "";

	// Adds a set of ads to the system
	public void initAds(Indigo stub, final List<String> ads, int numCopies, AtomicInteger counter, int total,
			String siteId) {
		try {
			AdServiceOps client = new AdServiceOps(stub, siteId);

			// Create ADs
			List<String> adsData = ads;
			int copies = numCopies;
			for (String line : adsData) {
				if (copies == numCopies) {
					client.addAd(line.split(";")[1]);
					copies = 0;
				} else {
					String[] args = line.split(";");
					client.addAdCopy(args[1]);
					copies++;
				}
			}

			System.err.printf("\rDone: %s", Progress.percentage(counter.incrementAndGet(), total));
			int txnSize = 0;

			stub.beginTxn();
			for (String line : adsData) {
				String[] lineSplit = line.split(";");
				if (copies == numCopies) {
					client.setAdInitialValue(lineSplit[1], Integer.parseInt(lineSplit[3]));
					copies = 0;
				} else {
					client.setAdCopyInitialValue(lineSplit[1], Integer.parseInt(lineSplit[3]));
					copies++;
				}

				if (txnSize >= 50) {
					stub.endTxn();
					stub.beginTxn();
					txnSize = 0;
				} else {
					txnSize++;
				}
			}
			stub.endTxn();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
}
