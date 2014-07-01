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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.thoughtworks.xstream.core.util.Base64Encoder;

abstract public class Workload implements Iterable<String>, Iterator<String> {

	private static final int MAX_SITES = 1321;
	/** List of user names */
	private static List<String> users = new ArrayList<String>();
	/** List of command line operations to generate user data */
	private static List<String> userData = new ArrayList<String>();

	/** Size of workload, i.e. number of operations */
	abstract public int size();

	/**
	 * Generates random user names and other (dummy, not semantically used)
	 * attributes such as password and date of birth. Uses a fixed,
	 * pre-determined random seed to ensure every site works on the same user
	 * data.
	 */
	public static void generateUsers(int numUsers) {
		System.err.println("Generating users and user data...");
		Random rg = new Random(6L);
		for (int i = 0; i < numUsers; i++) {
			byte[] tmp = new byte[6];
			rg.nextBytes(tmp);
			Base64Encoder enc = new Base64Encoder();

			String user = enc.encode(tmp);
			String userLine = String.format("usr_add;%s;passwd;\"%s %sson\";01/01/01;\"\";", user, user, user);
			users.add(user);
			userData.add(userLine);
		}
	}

	/**
	 * Represents an abstract command the user performs. Each command has a
	 * frequency/probability and needs to be formatted into a command line.
	 */
	static abstract class Operation {
		private int frequency;

		Operation freq(int f) {
			this.frequency = f;
			return this;
		}

		abstract String doLine(Random rg, String user, List<String> candidates);

		public String toString() {
			return getClass().getSimpleName();
		}
	}

	/**
	 * Status operation.
	 */
	static class Status extends Operation {
		String[] activities = new String[]{"Running Experiments", "Drinking Coffee", "Sleeping"};

		@Override
		String doLine(Random rg, String user, List<String> dummy) {
			int index = rg.nextInt(activities.length);
			return String.format("status;\"%s\";", activities[index]);
		}
	}

	/**
	 * Posts a message. Target is a randomly chosen user from a list of
	 * candidates (eg. friends).
	 */
	static class Post extends Operation {
		@Override
		public String doLine(Random rg, String user, List<String> candidates) {
			int index = rg.nextInt(candidates.size());
			String recipient = candidates.get(index);
			return String.format("post;%s;\"What up, dawg\";", recipient);
		}
	}

	/**
	 * Reads messages and events of a user. Target is a randomly chosen user
	 * from a list of candidates (eg. allusers).
	 */
	static class Read extends Operation {
		@Override
		public String doLine(Random rg, String user, List<String> candidates) {
			int index = rg.nextInt(candidates.size());
			String peer = candidates.get(index);
			return String.format("read;%s;", peer);
		}
	}

	/**
	 * Befriends a user. Target is randomly chosen from a list of candidates
	 * (eg. allusers).
	 */
	static class Friend extends Operation {
		@Override
		public String doLine(Random rg, String user, List<String> candidates) {
			int index = rg.nextInt(candidates.size());
			String peer = candidates.get(index);
			return String.format("friend;%s;", peer);
		}
	}

	/**
	 * Reads all friends of a user. Target is randomly chosen from a list of
	 * candidates (eg. friends).
	 */
	static class SeeFriends extends Operation {
		@Override
		public String doLine(Random rg, String user, List<String> candidates) {
			int index = rg.nextInt(candidates.size());
			String peer = candidates.get(index);
			return String.format("see_friends;%s;", peer);
		}
	}

	/**
	 * Login. Signals start of session.
	 */
	static class Login extends Operation {
		@Override
		public String doLine(Random rg, String user, List<String> dummy) {
			return String.format("login;%s;passwd;", user);
		}
	}

	/**
	 * Logout. Signals end of session.
	 */
	static class Logout extends Operation {
		@Override
		public String doLine(Random rg, String user, List<String> dummy) {
			return String.format("logout;%s;", user);
		}
	}

	/**
	 * Defines the set of available operations and their frequency. Frequencies
	 * need to add up to 100.
	 */
	private static Operation[] ops = new Operation[]{new Status().freq(5), new Post().freq(5), new Read().freq(80), new Friend().freq(2), new SeeFriends().freq(8)};

	private static AtomicInteger doMixedCounter = new AtomicInteger(7);

	/**
	 * Generates a workload with a mixture of operations. The workload
	 * represents a session for some randomly chosen user. Each session starts
	 * with a login and ends with a logout operations for this user.
	 * 
	 * @param site
	 *            from which to chose the user from, randomly chosen if site < 0
	 * @param friends_per_user
	 *            number of friends per user
	 * @param ops_biased
	 *            number of ops chosen with a bias to increase data locality
	 * @param ops_random
	 *            number of randomly chosen ops
	 * @param ops_groups
	 *            number of operation groups to generate
	 * @param number_of_sites
	 *            number of user partitions
	 * @return workload represented as an iterable collection of operations
	 */
	static public Workload doMixed(int site, int friends_per_user, final int ops_biased, final int ops_random, final int ops_groups, int number_of_sites) {
		// Each workload has its own seed...
		final Random rg = new Random(doMixedCounter.addAndGet(MAX_SITES + site));

		// Pick a user at random from this site's user partition
		site = site < 0 ? rg.nextInt(number_of_sites) : site; // fix site
		int partitionSize = users.size() / number_of_sites;
		final String user = users.get(rg.nextInt(partitionSize) + partitionSize * site);

		// Generate random friends for the user
		final List<String> friends = new ArrayList<String>();
		for (int i = 0; i < friends_per_user; i++)
			friends.add(users.get(rg.nextInt(users.size())));

		// Generate 100 biased operations, according to their frequency
		final List<String> mix = new ArrayList<String>();
		for (Operation i : ops)
			for (int j = 0; j < i.frequency; j++)
				mix.add(i.doLine(rg, user, friends));

		if (mix.size() != 100) {
			System.err.println("Workload generation bug");
			System.exit(0);
		}

		return new Workload() {
			int groupCounter = 0;
			Iterator<String> it = null;

			// operation groups are generated on the fly
			void refill() {
				ArrayList<String> group = new ArrayList<String>();

				// first group starts with login...
				if (groupCounter == 0)
					group.add(new Login().doLine(rg, user, null));

				if (groupCounter < ops_groups) {

					// append biased operations, those targeting the user's
					// friends
					for (int i = 0; i < ops_biased; i++)
						group.add(mix.get(rg.nextInt(mix.size())));

					// append read operations targeting any user in the system.
					for (int i = 0; i < ops_random; i++)
						group.add(new Read().doLine(rg, user, users));
				}

				groupCounter++;

				// last group ends with logout
				if (groupCounter == ops_groups)
					group.add(new Logout().doLine(rg, user, null));

				it = group.iterator();
			}

			@Override
			public boolean hasNext() {
				if (it == null || !it.hasNext())
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

			@Override
			public int size() {
				return 2 + ops_groups * (ops_biased + ops_random);
			}

		};
	}

	public Iterator<String> iterator() {
		return this;
	}

	public static void main(String[] args) throws Exception {
		int numUsers = 25000;
		Workload.generateUsers(numUsers);
		System.out.println("Generated " + numUsers + " users");

		Workload res = Workload.doMixed(0, 25, 9, 2, 2, 10);
		System.out.println("Generated " + res.size() + " operations");
		for (String i : res)
			System.out.println(i);

	}

	public static List<String> getUserData() {
		return userData;
	}
}
