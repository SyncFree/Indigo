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
package swift.application.social;

import static java.lang.System.exit;
import static sys.Context.Sys;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import swift.indigo.IndigoSequencer;
import swift.indigo.IndigoServer;
import swift.utils.SafeLog;
import sys.shepard.Shepard;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Progress;
import sys.utils.Props;
import sys.utils.Tasks;
import sys.utils.Threading;
/**
 * Benchmark of SwiftSocial, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftSocial sessions from the provided file. Sessions can be
 * distributed among different instances by specifying sessions range.
 */
public class SwiftSocialBenchmark extends SwiftSocialApp {

	private static String shepard;

	public void initDB(String[] args) {

		String propFile = Args.valueOf(args, "-props", "src-app/swift/application/social/swiftsocial-test.props");
		Properties properties = Props.parseFile("swiftsocial", propFile);

		System.err.println("Populating db with users...");

		int numUsers = Props.intValue(properties, "swiftsocial.numUsers", 1000);

		final int NumUsers = Args.valueOf(args, "-users", numUsers);
		Workload.generateUsers(NumUsers);

		int threads = Args.valueOf(args, "-threads", 2);
		final int PARTITION_SIZE = 1000;
		int partitions = numUsers / PARTITION_SIZE + (numUsers % PARTITION_SIZE > 0 ? 1 : 0);
		ExecutorService pool = Executors.newFixedThreadPool(threads);

		final AtomicInteger counter = new AtomicInteger(0);
		for (int i = 0; i < partitions; i++) {
			int lo = i * PARTITION_SIZE, hi = (i + 1) * PARTITION_SIZE;
			final List<String> partition = Workload.getUserData().subList(lo, Math.min(hi, numUsers));
			pool.execute(new Runnable() {
				public void run() {
					SwiftSocialBenchmark.super.initUsers(partition, counter, NumUsers);
				}
			});
		}
		Threading.awaitTermination(pool, Integer.MAX_VALUE);
		Threading.sleep(5000);
		System.err.println("\nFinished populating db with users.");
	}

	public void doBenchmark(String[] args) {
		super.init(args);
		// IO.redirect("stdout.txt", "stderr.txt");
		final long startTime = System.currentTimeMillis();

		System.err.println(IP.localHostname() + "/ starting...");

		int concurrentSessions = Args.valueOf(args, "-threads", 1);

		String partitions = Args.valueOf(args, "-partition", "0/1");
		int site = Integer.valueOf(partitions.split("/")[0]);
		int numberOfSites = Integer.valueOf(partitions.split("/")[1]);
		// ASSUMPTION: concurrentSessions is the same at all sites
		int numberOfVirtualSites = numberOfSites * concurrentSessions;

		shepard = Args.valueOf(args, "-shepard", "");

		System.err.println(IP.localHostAddress() + " connecting to: " + server);

		super.populateWorkloadFromConfig();

		SafeLog.printfComment("\n");
		SafeLog.printfComment("\targs=%s\n", Arrays.asList(args));
		SafeLog.printfComment("\tsite=%s\n", site);
		SafeLog.printfComment("\tnumberOfSites=%s\n", numberOfSites);
		SafeLog.printfComment("\tthreads=%s\n", concurrentSessions);
		SafeLog.printfComment("\tnumberOfVirtualSites=%s\n", numberOfVirtualSites);
		SafeLog.printfComment("\tSurrogate=%s\n", server);
		SafeLog.printfComment("\tShepard=%s\n", shepard);
		SafeLog.printHeader();

		if (!shepard.isEmpty())
			Shepard.sheepJoinHerd(shepard);

		// Kick off all sessions, throughput is limited by
		// concurrentSessions.
		final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentSessions, Threading.factory("App"));

		System.err.println("Spawning session threads.");
		for (int i = 0; i < concurrentSessions; i++) {
			final int sessionId = site * concurrentSessions + i;
			final Workload commands = getWorkloadFromConfig(sessionId, numberOfVirtualSites);
			threadPool.execute(new Runnable() {
				public void run() {
					// Randomize startup to avoid clients running all at the
					// same time; causes problems akin to DDOS symptoms.
					Threading.sleep(Sys.random().nextInt(1000));
					SwiftSocialBenchmark.super.runClientSession(Integer.toString(sessionId), commands, false);
				}
			});
		}

		Tasks.every(1.0, () -> {
			System.err.printf("Done: %s\n", Progress.percentage(commandsDone.get(), totalCommands.get()));
		});

		// Wait for all sessions.
		threadPool.shutdown();
		Threading.awaitTermination(threadPool, Integer.MAX_VALUE);

		System.err.println("Session threads completed.");
		System.err.println("Throughput: " + totalCommands.get() * 1000 / (System.currentTimeMillis() - startTime) + " txns/s");
		System.exit(0);
	}

	public static void main(String[] args) {
		Args.use(args);

		SwiftSocialBenchmark instance = new SwiftSocialBenchmark();
		if (args.length == 0) {

			IndigoSequencer.main(new String[]{"-name", "X"});
			IndigoServer.main(new String[]{"-servers", "localhost"});

			args = new String[]{"-server", "localhost", "-threads", "5", "-props", "src-app/swift/application/social/swiftsocial-test.props"};
			instance.initDB(args);
			instance.doBenchmark(args);
			exit(0);
		}

		if (Args.contains("init")) {
			instance.initDB(args);
			exit(0);
		}
		if (Args.contains("run")) {
			instance.doBenchmark(args);
			exit(0);
		}
		System.err.println("nothing to do...");
	}
}
