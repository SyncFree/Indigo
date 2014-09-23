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
package indigo.application.tournament;

import static java.lang.System.exit;
import static sys.Context.Networking;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import swift.indigo.Defaults;
import swift.indigo.Indigo;
import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.IndigoServer;
import swift.indigo.remote.RemoteIndigo;
import sys.shepard.PatientShepard;
import sys.utils.Args;
import sys.utils.IP;
import sys.utils.Progress;
import sys.utils.Tasks;
import sys.utils.Threading;

/**
 * Benchmark of SwiftAdSerive, based on data model derived from WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 * <p>
 * Runs in parallel SwiftAdSerive sessions from the provided file. Sessions can
 * be distributed among different instances by specifying sessions range.
 */
public class TournamentServiceBenchmark extends TournamentServiceApp {
	private static Logger Log = Logger.getLogger(TournamentServiceBenchmark.class.getName());

	public void initDB(String[] args) {

		final String siteId = Args.valueOf(args, "-siteId", "X");
		final String master = Args.valueOf(args, "-master", "X");
		final String surrogate = Args.valueOf(args, "-srvAddress", "tcp://*/36001/");

		List<String> commands = super.populateWorkloadFromConfig(master);
		final int N_INIT_WORKERS = 4;
		int partitionSize = (int) Math.ceil(commands.size() / N_INIT_WORKERS);
		ExecutorService threadPool = Executors.newFixedThreadPool(N_INIT_WORKERS);

		final AtomicInteger counter = new AtomicInteger(0);
		for (int i = 0; i < N_INIT_WORKERS; i++) {
			int lo = i * partitionSize, hi = (i + 1) * partitionSize;
			final List<String> partition = commands.subList(lo, Math.min(hi, commands.size()));
			threadPool.execute(new Runnable() {
				public void run() {
					final Indigo stub = RemoteIndigo.getInstance(Networking.resolve(surrogate, Defaults.REMOTE_INDIGO_URL));
					TournamentServiceBenchmark.super.initTournaments(stub, partition, counter, numOps, siteId, master);
				}
			});
		}
		// Wait for all sessions.
		Threading.awaitTermination(threadPool, Integer.MAX_VALUE);
		Log.info("\nFinished populating db with players/tournaments.");
	}

	public void doBenchmark(String[] args) {

		Args.use(args);
		final String siteId = Args.valueOf(args, "-siteId", "X");
		final String master = Args.valueOf(args, "-master", "X");
		final String server = Args.valueOf(args, "-srvAddress", "tcp://*/36001/");

		Log.info(IP.localHostname() + "/ starting...");

		int concurrentSessions = Args.valueOf(args, "-threads", 1);

		if (Args.contains("-shepard")) {
			PatientShepard.sheepJoinHerd(Args.valueOf("-shepard", ""));
		};

		Log.info(IP.localHostAddress() + " connecting to: " + server);

		populateWorkloadFromConfig(master);

		// bufferedOutput.printf(";\n;\targs=%s\n", Arrays.asList(args));
		// bufferedOutput.printf(";\tSurrogate=%s\n", server);
		// bufferedOutput.printf(";\tthreads=%s\n;\n", concurrentSessions);

		// Kick off all sessions, throughput is limited by
		// concurrentSessions.
		final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentSessions, Threading.factory("X"));

		Log.info("Spawning session threads.");
		for (int i = 0; i < concurrentSessions; i++) {
			final int sessionId = i;
			final Workload commands = super.getWorkloadFromConfig(siteId, master);

			threadPool.execute(new Runnable() {
				public void run() {
					// Randomize startup to avoid clients running all at the
					// same time; avoid problems akin to DDOS symptoms.
					Threading.sleep(new Random().nextInt(1000));
					Indigo stub = RemoteIndigo.getInstance(Networking.resolve(server, "tcp://*/36001/"));
					TournamentServiceBenchmark.super.runClientSession(new TournamentServiceOps(stub, siteId, master), sessionId, commands, false);
				}
			});
		}

		Tasks.every(0.0, 1.0, () -> {
			Log.info(Progress.percentage(commandsDone.get(), totalCommands.get()));
		});

		// Wait for all sessions.
		Threading.awaitTermination(threadPool, Integer.MAX_VALUE);
		Log.info("Session threads completed.");
	}

	public static void main(String[] args) {
		Args.use(args);
		TournamentServiceBenchmark instance = new TournamentServiceBenchmark();
		if (args.length == 0) {

			IndigoSequencerAndResourceManager.main(new String[]{"-siteId", "X"});
			IndigoServer.main(new String[]{"-siteId", "X"});

			args = new String[]{"-server", "localhost", "-name", "X", "-threads", "1"};

			instance.initDB(args);
			instance.doBenchmark(args);
			exit(0);
		}

		if (args[0].equals("-init")) {
			instance.initDB(args);
			exit(0);
		}
		if (args[0].equals("-run")) {
			instance.doBenchmark(args);
			exit(0);
		}
	}
}
