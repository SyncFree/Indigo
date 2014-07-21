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
import static sys.Sys.Sys;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import swift.indigo.Indigo;
import swift.indigo.IndigoAppServer;
import swift.indigo.IndigoSequencerAndResourceManager;
import swift.indigo.remote.RemoteIndigo;
import sys.scheduler.PeriodicTask;
import sys.shepard.Shepard;
import sys.utils.Args;
import sys.utils.IP;
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

    private String shepard;

    public void initDB(String[] args) {

        final Indigo stub = RemoteIndigo.getInstance(Args.valueOf(args, "-server", "localhost"));
        final String siteId = Args.valueOf(args, "-name", "X0");

        String sitesArg = Args.valueOf(args, "-partition", "0/1");
        int numberOfSites = Integer.valueOf(sitesArg.split("/")[1]);

        List<String> players = super.populateWorkloadFromConfig(numberOfSites);

        final int PARTITION_SIZE = 1000;
        int partitions = (int) Math.ceil(players.size() / (double) PARTITION_SIZE);
        ExecutorService threadPool = Executors.newFixedThreadPool(4);

        final AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < partitions; i++) {
            int lo = i * PARTITION_SIZE, hi = (i + 1) * PARTITION_SIZE;
            final List<String> partition = players.subList(lo, Math.min(hi, players.size()));
            threadPool.execute(new Runnable() {
                public void run() {
                    TournamentServiceBenchmark.super.initTournaments(stub, partition, counter, numOps, siteId);
                }
            });
        }
        // Wait for all sessions.
        Threading.awaitTermination(threadPool, Integer.MAX_VALUE);
        Log.info("\nFinished populating db with players/tournaments.");
    }

    public void doBenchmark(String[] args) {

        // IO.redirect("stdout.txt", "stderr.txt");

        final String siteId = Args.valueOf(args, "-name", "X0");
        final String server = Args.valueOf(args, "-server", "localhost");

        Log.info(IP.localHostname() + "/ starting...");

        int concurrentSessions = Args.valueOf(args, "-threads", 1);
        String partitions = Args.valueOf(args, "-partition", "0/1");
        int site = Integer.valueOf(partitions.split("/")[0]);
        int numberOfSites = Integer.valueOf(partitions.split("/")[1]);

        shepard = Args.valueOf(args, "-shepard", "");

        Log.info(IP.localHostAddress() + " connecting to: " + server);

        populateWorkloadFromConfig(numberOfSites);

        bufferedOutput.printf(";\n;\targs=%s\n", Arrays.asList(args));
        bufferedOutput.printf(";\tsite=%s\n", site);
        bufferedOutput.printf(";\tnumberOfSites=%s\n", numberOfSites);
        bufferedOutput.printf(";\tSurrogate=%s\n", server);
        bufferedOutput.printf(";\tShepard=%s\n", shepard);
        bufferedOutput.printf(";\tthreads=%s\n;\n", concurrentSessions);

        if (!shepard.isEmpty())
            Shepard.sheepJoinHerd(shepard);

        // Kick off all sessions, throughput is limited by
        // concurrentSessions.
        final ExecutorService threadPool = Executors.newFixedThreadPool(concurrentSessions, Threading.factory("App"));

        Log.info("Spawning session threads.");
        for (int i = 0; i < concurrentSessions; i++) {
            final int sessionId = i;
            final Workload commands = super.getWorkloadFromConfig(site, numberOfSites);

            threadPool.execute(new Runnable() {
                public void run() {
                    // Randomize startup to avoid clients running all at the
                    // same time; avoid problems akin to DDOS symptoms.
                    Threading.sleep(Sys.rg.nextInt(1000));
                    Indigo stub = RemoteIndigo.getInstance(server);
                    TournamentServiceOps serviceClient = new TournamentServiceOps(stub, siteId);
                    TournamentServiceBenchmark.super.runClientSession(serviceClient, sessionId, commands, false);
                }
            });
        }

        new PeriodicTask(0.0, 1.0) {
            String prev = "";

            public void run() {
                int done = commandsDone.get();
                int total = totalCommands.get();
                String curr = String.format("--->DONE: %.1f%%, %d/%d\n", 100.0 * done / total, done, total);
                if (!curr.equals(prev)) {
                    Log.info(curr);
                    prev = curr;
                }
            }
        };

        // Wait for all sessions.
        Threading.awaitTermination(threadPool, Integer.MAX_VALUE);
        Log.info("Session threads completed.");
    }

    public static void main(String[] args) {
        sys.Sys.init();

        TournamentServiceBenchmark instance = new TournamentServiceBenchmark();
        if (args.length == 0) {

            IndigoSequencerAndResourceManager.main(new String[] { "-name", "X0" });
            IndigoAppServer.main(new String[] { "localhost" });

            args = new String[] { "-server", "localhost", "-name", "X0" };

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