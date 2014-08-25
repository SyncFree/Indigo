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

import indigo.application.adservice.Results;

import java.io.PrintStream;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import swift.exceptions.SwiftException;
import swift.indigo.Indigo;
import swift.utils.Pair;
import sys.utils.Props;
import sys.utils.Threading;

/**
 * Executing SwiftAdService operations, based on data model of WaltSocial
 * prototype [Sovran et al. SOSP 2011].
 */
public class TournamentServiceApp {
	private static Logger Log = Logger.getLogger(TournamentServiceApp.class.getName());

	protected int thinkTime;
	protected int numPlayers;
	protected int globalTournaments;
	protected int numLocalTournaments;
	protected int maxPlayers;
	protected int numOps;
	protected int localPercentage;

	protected PrintStream bufferedOutput;

	protected AtomicInteger commandsDone = new AtomicInteger(0);
	protected AtomicInteger totalCommands = new AtomicInteger(0);
	private Properties props;

	// If number of sites is defined in the config file, than overrides the
	// parameter
	public List<String> populateWorkloadFromConfig(int numberOfSites) {

		bufferedOutput = new PrintStream(System.out, false);

		props = Props.parseFile("indigo-tournament", bufferedOutput, "indigo-tournament-test.props");

		numPlayers = Props.intValue(props, "tournament.numPlayers", 10);
		globalTournaments = Props.intValue(props, "tournament.numGlobalTournaments", 10);
		numLocalTournaments = Props.intValue(props, "tournament.numLocalTournaments", 100);
		maxPlayers = Props.intValue(props, "tournament.maxPlayers", 100);
		numOps = Props.intValue(props, "tournament.numOps", 1000);
		thinkTime = Props.intValue(props, "tournament.thinkTime", 1000);
		numberOfSites = Props.intValue(props, "tournament.numberOfSites", numberOfSites);

		return Workload.populate(numPlayers, numLocalTournaments, globalTournaments, maxPlayers, numberOfSites);
	}

	public Workload getWorkloadFromConfig(int site, int numberOfSites) {
		if (props == null)
			populateWorkloadFromConfig(numberOfSites);

		return Workload.doMixed(site, numOps, localPercentage, numberOfSites);
	}

	public void runClientSession(TournamentServiceOps serviceClient, final int sessionId, final Workload commands,
			boolean loop4Ever) {

		totalCommands.addAndGet(commands.size());
		final long sessionStartTime = System.currentTimeMillis();
		final String initSessionLog = String.format("%d,%s,%d,%d", -1, "INIT", 0, sessionStartTime);
		bufferedOutput.println(initSessionLog);
		if (sessionId == 0)
			bufferedOutput.println("; sessionId,responseCode,copyValue,globalValue,execTime,endTime");
		do
			for (String cmdLine : commands) {
				long txnStartTime = System.currentTimeMillis();
				Results res = runCommandLine(sessionId, serviceClient, cmdLine);
				res.setStartTime(txnStartTime).setSession(sessionId).logTo(bufferedOutput);
				Threading.sleep(thinkTime);
				commandsDone.incrementAndGet();
			}
		while (loop4Ever);

		final long now = System.currentTimeMillis();
		final long sessionExecTime = now - sessionStartTime;
		bufferedOutput.println(String.format("%d,%s,%d,%d", sessionId, "TOTAL", sessionExecTime, now));
		bufferedOutput.flush();
	}

	//
	public Results runCommandLine(int sessionId, TournamentServiceOps tournamentClient, String cmdLine) {
		String[] toks = cmdLine.split(";");
		final Commands cmd = Commands.valueOf(toks[0].toUpperCase());
		Results result = null;
		try {
			switch (cmd) {
				case ADD_PLAYER :
					if (toks.length == 3) {
						String playerName = tournamentClient.newName(6);
						tournamentClient.addPlayer(Integer.parseInt(toks[1]), playerName);
					}
				case ADD_TOURNAMENT :
					if (toks.length == 3) {
						int tournametSiteId = toks[2].equals("GLOBAL") ? -1 : Integer.parseInt(toks[1]);
						String tournament = tournamentClient.newName(6);
						tournamentClient.addTournament(tournametSiteId, tournament, maxPlayers);
						break;
					}
				case REM_TOURNAMENT :
					if (toks.length == 3) {
						int siteId = toks[2].equals("GLOBAL") ? -1 : Integer.parseInt(toks[1]);
						String tournament = tournamentClient.selectTournament(siteId);
						tournamentClient.removeTournament(siteId, tournament);
						break;
					}
				case ENROLL_TOURNAMENT :
					if (toks.length == 3) {
						String player = tournamentClient.selectPlayer(Integer.parseInt(toks[1]));
						if (player == null) {
							Log.info("No player available at site " + toks[1]);
							break;
						}
						String tournament = tournamentClient.selectTournament(toks[2].equals("GLOBAL") ? -1 : Integer
								.parseInt(toks[1]));
						tournamentClient.enrollTournament(player, tournament, maxPlayers);
						break;
					}
				case DISENROLL_TOURNAMENT :
					if (toks.length == 3) {
						String player = tournamentClient.selectPlayer(Integer.parseInt(toks[1]));
						String tournament = tournamentClient.selectTournament(toks[2].equals("GLOBAL") ? -1 : Integer
								.parseInt(toks[1]));
						if (player == null || tournament == null) {
							Log.info("No player or tournament available at site " + toks[1]);
							break;
						}
						tournamentClient.disenrollTournament(player, tournament);
						break;
					}
				case DO_MATCH :
					if (toks.length == 3) {
						int site = toks[2].equals("GLOBAL") ? -1 : Integer.parseInt(toks[1]);
						String tournament = tournamentClient.selectTournament(site);
						Pair<String, String> players = tournamentClient.selectTournamentPlayerPair(tournament);
						// TODO: Does not handle unique identifiers
						if (players != null) {
							tournamentClient.doMatch(UUID.randomUUID().toString(), tournament, players.getFirst(),
									players.getSecond());
						} else {
							// Output different message here
						}
						break;
					}
				case VIEW_STATUS :
					if (toks.length == 3) {
						int site = toks[2].equals("GLOBAL") ? -1 : Integer.parseInt(toks[1]);
						String tournament = tournamentClient.selectTournament(site);
						if (tournament == null) {
							Log.info("No tournament available at site " + toks[1]);
							break;
						}
						tournamentClient.viewStatus(tournament);
						break;
					}
				default :
					System.err.println("Can't parse command line :" + cmdLine);
					System.err.println("Exiting...");
					System.exit(1);
			}
		} catch (SwiftException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return new TournamentOpsResults(cmd.toString());
	}

	String progressMsg = "";

	// Adds a set of tournaments to the system
	public void initTournaments(Indigo stub, final List<String> commands, AtomicInteger counter, int total,
			String siteId) {
		try {
			TournamentServiceOps client = new TournamentServiceOps(stub, siteId);

			for (String line : commands) {

				String msg = String.format("Initialization:%.0f%%", 100.0 * counter.incrementAndGet() / total);
				if (!msg.equals(progressMsg)) {
					progressMsg = msg;
					Log.info(progressMsg);
				}
				String[] toks = line.split(";");
				String[] tournament = toks[0].split("_");
				int tournamentSite = Integer.parseInt(tournament[0]);
				client.addTournament(tournamentSite, tournament[1], maxPlayers);
				String[] players = new String[toks.length - 1];
				int playerSite = Integer.parseInt(toks[1].split("_")[0]);
				for (int i = 1; i < toks.length; i++) {
					String[] playerToks = toks[i].split("_");
					players[i - 1] = playerToks[1];
				}
				client.addNewPlayersToTournament(players, playerSite, tournament[1]);
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	static class TournamentOpsResults implements Results {

		long txnStartTime, txnEndTime;
		int sessionId;
		String cmd;

		protected TournamentOpsResults(String cmd) {
			this.cmd = cmd;
		}

		public Results setStartTime(long start) {
			this.txnStartTime = start;
			this.txnEndTime = System.currentTimeMillis();
			return this;
		}

		@Override
		public Results setSession(int sessionId) {
			this.sessionId = sessionId;
			return this;
		}

		public String getLogRecord() {
			final long txnExecTime = txnEndTime - txnStartTime;
			return String.format("%d,%s,%d,%d", sessionId, cmd.toString(), txnExecTime, txnEndTime);
		}

		@Override
		public void logTo(PrintStream out) {
			out.println(getLogRecord());
		}

	}
}
