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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.thoughtworks.xstream.core.util.Base64Encoder;

abstract public class Workload implements Iterable<String>, Iterator<String> {

	static List<String> players = new ArrayList<String>();
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

	public static List<String> populate(int numPlayers, int numLocalTournaments, int numGlobalTournaments,
			int maxPlayers, int numSites) {
		Random rg = new Random(6L);
		byte[] tmp = new byte[6];
		Base64Encoder enc = new Base64Encoder();
		int currSite = -1;
		int playersPerSite = numPlayers / numSites;
		int tournamentsPerSite = numLocalTournaments / numSites;
		for (int i = 0; i < numPlayers; i++) {
			currSite = (i % playersPerSite == 0) ? currSite + 1 : currSite;
			rg.nextBytes(tmp);
			String player = enc.encode(tmp);
			players.add((currSite + 1) + "_" + player);
		}

		currSite = -1;
		for (int i = 0; i < numLocalTournaments; i++) {
			currSite = (i % tournamentsPerSite == 0) ? currSite + 1 : currSite;
			rg.nextBytes(tmp);
			String tournament = enc.encode(tmp);
			tournaments.add((currSite + 1) + "_" + tournament);
		}

		currSite = -1;
		for (int i = 0; i < numLocalTournaments; i++) {
			currSite = (i % tournamentsPerSite == 0) ? currSite + 1 : currSite;
			int tournamentPlayers = maxPlayers / 2 + rg.nextInt(maxPlayers / 2);
			StringBuffer line = new StringBuffer();
			line.append(tournaments.get(i));
			Set<Integer> playersInTournament = new HashSet<>();
			for (int j = 0; j < tournamentPlayers; j++) {
				int random = rg.nextInt(numPlayers / numSites);
				if (!playersInTournament.contains(random)) {
					String p = players.get(((numPlayers / numSites) * currSite)) + random;
					line.append(";" + p);
					playersInTournament.add(random);
				} else {
					j--;
				}
			}
			playersData.add(line.toString());
		}

		// GLOBAL TOURNAMENTS
		for (int i = 0; i < numGlobalTournaments; i++) {
			rg.nextBytes(tmp);
			String tournament = "-1_" + enc.encode(tmp);
			int tournamentPlayers = maxPlayers / 2 + rg.nextInt(maxPlayers / 2);
			StringBuffer line = new StringBuffer();
			line.append(tournament);
			for (int j = 0; j < tournamentPlayers; j++) {
				String p = players.get(rg.nextInt(numPlayers));
				line.append(";" + p);
			}
			playersData.add(line.toString());
		}

		return playersData;
	}
	/*
	 * Represents an abstract command the user performs. Each command has a
	 * frequency/probability and need to be formatted into a command line.
	 */
	static abstract class Operation {
		int frequency;

		Operation freq(int f) {
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

	static Operation[] ops = new Operation[]{new AddPlayer().freq(5), new AddTournament().freq(2),
			new EnrollTournament().freq(16), new DisenrollTournament().freq(4), new DoMatch().freq(22),
			new RemTournament().freq(1), new ViewStatus().freq(50)};

	static AtomicInteger doMixedCounter = new AtomicInteger(7);

	static public Workload doMixed(int site, final int totalOps, final int localPercentage, int number_of_sites) {
		final Random rg = new Random(doMixedCounter.addAndGet(13 + site));
		final int finalSite = site < 0 ? rg.nextInt(number_of_sites) : site; // fix
																				// site

		// Generate the biased operations, according to their frequency
		final List<String> mix = new ArrayList<String>();
		for (Operation i : ops)
			for (int j = 0; j < i.frequency; j++)
				mix.add(i.doLine(rg));

		if (mix.size() != 100) {
			System.err.println("Workload generation bug");
			System.exit(0);
		}

		return new Workload() {
			Iterator<String> it = null;

			void refill() {
				ArrayList<String> group = new ArrayList<String>();
				for (int i = 0; i < totalOps; i++) {
					if (rg.nextInt(100) > localPercentage) {
						// Global OP
						group.add(mix.get(rg.nextInt(mix.size())) + ";" + finalSite + ";GLOBAL");
					} else {
						group.add(mix.get(rg.nextInt(mix.size())) + ";" + finalSite + ";LOCAL");
					}
				}
				it = group.iterator();
			}

			@Override
			public boolean hasNext() {
				if (it == null)
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
		List<String> x = Workload.populate(30, 9, 3, 4, 3);
		System.out.println(players);
		System.out.println(tournaments);
		for (String i : x)
			System.out.println(i);

		Workload res = Workload.doMixed(3, 100, 80, 3);
		System.out.println(res.size());
		for (String i : res)
			System.out.println(i);

	}
}
