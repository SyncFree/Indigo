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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import sys.utils.Props;

import com.thoughtworks.xstream.core.util.Base64Encoder;

abstract public class Workload implements Iterable<String>, Iterator<String> {

	static List<String> players = new ArrayList<String>();
	static List<String> playersInLocalTournamnet = new ArrayList<String>();
	static List<String> tournaments = new ArrayList<String>();
	static List<String> playersData = new ArrayList<String>();
	static AtomicInteger matchCounter = new AtomicInteger();

	protected Workload() {
	}

	abstract public int size();

	/*
	 * Distributes the numLocalTournaments between sites. Distributes the
	 * players among the tournaments of the local sites The number of players in
	 * each tournament is at least half of the limit
	 */

	public static List<String> populate(int numPlayers, int numLocalTournaments, int numGlobalTournaments, int minPlayersLocal, int maxPlayersLocal, int minPlayersGlobal, int maxPlayersGlobal, int nSites, String master) {
		Random rg = new Random(6L);
		byte[] tmp = new byte[6];
		Base64Encoder enc = new Base64Encoder();
		int currSite = -1;
		int playersPerSite = numPlayers / nSites;
		int tournamentsPerSite = numLocalTournaments / nSites;
		List<String> globalTournaments = new ArrayList<>();

		// Create players
		for (int i = 0; i < numPlayers; i++) {
			currSite = i / playersPerSite;
			if (currSite >= nSites) {
				// To add the remaining elements
				currSite = nSites - 1;
			}
			rg.nextBytes(tmp);
			String player = enc.encode(tmp);
			players.add((currSite + 1) + "_" + player);
		}

		// Create Tournaments
		for (int i = 0; i < numLocalTournaments; i++) {
			currSite = i / tournamentsPerSite;
			if (currSite >= nSites) {
				// To add the remaining elements
				currSite = nSites - 1;
			}
			rg.nextBytes(tmp);
			String tournament = enc.encode(tmp);
			tournaments.add((currSite + 1) + "_" + tournament);
		}

		for (int i = 0; i < numGlobalTournaments; i++) {
			rg.nextBytes(tmp);
			String tournament = enc.encode(tmp);
			globalTournaments.add("-1_" + tournament);
		}

		// Add players to tournaments
		for (int i = 0; i < numLocalTournaments; i++) {
			currSite = i / tournamentsPerSite;
			if (currSite >= nSites) {
				// To add the remaining elements
				currSite = nSites - 1;
			}
			int numPlayersTournament = Math.min(minPlayersLocal + rg.nextInt(maxPlayersLocal - minPlayersLocal), playersPerSite);
			StringBuffer line = new StringBuffer();
			line.append(tournaments.get(i));
			Set<Integer> playersInTournament = new HashSet<>();
			for (int j = 0; j < numPlayersTournament; j++) {
				int random = (currSite * playersPerSite) + rg.nextInt(playersPerSite);
				if (!playersInTournament.contains(random)) {
					String p = players.get(random);
					line.append(";" + p);
					playersInTournament.add(random);
					playersInLocalTournamnet.add(p);
				} else {
					j--;
				}
			}
			playersData.add(line.toString());
		}

		// Add players to global tournaments
		for (int i = 0; i < numGlobalTournaments; i++) {
			String tournament = globalTournaments.get(i);
			int numPlayersTournament = Math.min(minPlayersGlobal + rg.nextInt(maxPlayersGlobal - minPlayersGlobal), numPlayers);
			StringBuffer line = new StringBuffer();
			line.append(tournament);
			Set<Integer> playersInTournament = new HashSet<>();
			for (int j = 0; j < numPlayersTournament; j++) {
				int random = rg.nextInt(playersInLocalTournamnet.size());
				if (!playersInTournament.contains(random)) {
					String p = playersInLocalTournamnet.get(random);
					line.append(";" + p);
					playersInTournament.add(random);
				} else {
					j--;
				}
			}
			playersData.add(line.toString());
		}

		tournaments.addAll(0, globalTournaments);
		return playersData;
	}
	/*
	 * Represents an abstract command the user performs. Each command has a
	 * frequency/probability and need to be formatted into a command line.
	 */
	static abstract class Operation {
		double frequency;

		Operation freq(double f) {
			this.frequency = f;
			return this;
		}

		abstract String doLine(Random rg);

		public String toString() {
			return getClass().getSimpleName();
		}
	}

	/*
	 * A Match between two random tournaments.
	 */
	static class DoMatch extends Operation {

		public String doLine(Random rg) {
			return String.format("do_match");
		}
	}

	static class ViewStatus extends Operation {

		public String doLine(Random rg) {
			return String.format("view_status");
		}
	}

	static class AddPlayer extends Operation {

		public String doLine(Random rg) {
			return String.format("add_player");
		}
	}

	/*
	 * Add a new tournament to the tournament
	 */
	static class AddTournament extends Operation {

		public String doLine(Random rg) {
			return String.format("add_tournament");
		}
	}

	static class RemTournament extends Operation {

		public String doLine(Random rg) {
			return String.format("rem_tournament");
		}
	}

	static class EnrollTournament extends Operation {

		public String doLine(Random rg) {
			return String.format("enroll_tournament");
		}
	}

	static class DisenrollTournament extends Operation {

		public String doLine(Random rg) {
			return String.format("disenroll_tournament");
		}
	}
	static Properties props = Props.parseFile("indigo-tournament", System.err, "indigo-tournament.props");

	static Operation[] ops = new Operation[]{new AddPlayer().freq(Double.parseDouble(Props.stringValue(props, "tournament.freq.addPlayers", "0"))),
			new AddTournament().freq(Double.parseDouble(Props.stringValue(props, "tournament.freq.addTournament", "0"))), new EnrollTournament().freq(Double.parseDouble(Props.stringValue(props, "tournament.freq.enrollTournament", "0"))),
			new DisenrollTournament().freq(Double.parseDouble(Props.stringValue(props, "tournament.freq.disenrollTournament", "0"))), new DoMatch().freq(Double.parseDouble(Props.stringValue(props, "tournament.freq.doMatch", "0"))),
			new RemTournament().freq(Double.parseDouble(Props.stringValue(props, "tournament.freq.remTournament", "0"))), new ViewStatus().freq(Double.parseDouble(Props.stringValue(props, "tournament.freq.viewStatus", "0")))};

	static AtomicInteger doMixedCounter = new AtomicInteger(7);

	static public Workload doMixed(int site, String siteName, final int totalOps, final int localPercentage) {
		final Random rg = new Random(doMixedCounter.addAndGet(13 + site));
		// Generate the biased operations, according to their frequency
		final List<String> mix = new ArrayList<String>();
		for (Operation i : ops) {
			int nOps = (int) ((i.frequency * 100));
			for (int j = 0; j < nOps; j++)
				mix.add(i.doLine(rg));
		}
		if (mix.size() != 10000) {
			System.err.println("Workload generation bug " + mix.size() + " minimum 1000 ops");
			System.exit(0);
		}

		return new Workload() {
			Iterator<String> it = null;
			int total = 0;

			void refill() {
				total += 10000;
				ArrayList<String> group = new ArrayList<String>();
				for (int i = 0; i < 10000; i++) {
					if (rg.nextInt(100) > localPercentage) {
						// Global OP
						group.add(mix.get(rg.nextInt(mix.size())) + ";" + site + ";GLOBAL");
					} else {
						group.add(mix.get(rg.nextInt(mix.size())) + ";" + site + ";LOCAL");
					}
				}
				it = group.iterator();
			}

			@Override
			public boolean hasNext() {
				if (it == null && total < totalOps)
					refill();

				return it.hasNext();
			}

			@Override
			public String next() {
				return it.next();
			}

			@Override
			public void remove() {
				throw new RuntimeException("On demand workload generation; remove is not supported...");
			}

			public int size() {
				return totalOps;
			}

		};
	}

	public Iterator<String> iterator() {
		return this;
	}

	public static void main(String[] args) throws Exception {

		List<String> x = Workload.populate(30, 9, 2, 10, 15, 25, 30, 3, "US-EAST");
		System.out.println(players);
		System.out.println(tournaments);
		for (String i : x)
			System.out.println(i);

		Workload res = Workload.doMixed(1, "US-EAST", 1000, 80);
		System.out.println(res.size());
		for (String i : res)
			System.out.println(i);

	}
}
