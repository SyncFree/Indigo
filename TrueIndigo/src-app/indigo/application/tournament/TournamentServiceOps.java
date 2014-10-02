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

import static sys.Context.Networking;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDTIdentifier;
import swift.crdt.AddWinsSetCRDT;
import swift.crdt.BoundedCounterAsResource;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.ShareableLock;
import swift.dc.Defaults;
import swift.exceptions.SwiftException;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.LockReservation;
import swift.indigo.ResourceRequest;
import swift.proto.CurrentClockRequest;
import swift.utils.Pair;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.utils.Args;

import com.thoughtworks.xstream.core.util.Base64Encoder;

// implements the ad service functionality

public class TournamentServiceOps {

	private static Logger logger = Logger.getLogger("indigo.tournament");

	private Indigo stub;
	final private String siteId;
	final private String master;
	final private String[] allSites;

	private Random rg;

	private Service fakeStub;

	private Endpoint surrogate;

	public TournamentServiceOps(Indigo stub, String siteId, String master, String[] allSites) {
		this.stub = stub;
		this.siteId = siteId;
		this.master = master;
		this.allSites = allSites;
		this.rg = new Random();
		fakeStub = Args.contains("-fakeCS") ? Networking.stub() : null;
		surrogate = Networking.resolve(Args.valueOf("-fakeCS", ""), Defaults.SERVER_URL);

	}

	protected String newName(int length) {
		byte[] tmp = new byte[length];
		rg.nextBytes(tmp);
		Base64Encoder enc = new Base64Encoder();
		String name = enc.encode(tmp);
		return name;
	}

	@SuppressWarnings("unchecked")
	protected String selectPlayer(int site) throws SwiftException {
		stub.beginTxn();
		AddWinsSetCRDT<String> playerIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerIndex(site + ""), true, AddWinsSetCRDT.class);
		stub.endTxn();
		String[] players = playerIndex.getValue().toArray(new String[0]);
		if (players.length > 0)
			return players[rg.nextInt(players.length)];
		else
			return null;

	}

	@SuppressWarnings("unchecked")
	protected String selectTournament(int site) throws SwiftException {
		stub.beginTxn();
		AddWinsSetCRDT<String> tournamentIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournamentIndex(site + ""), true, AddWinsSetCRDT.class);
		stub.endTxn();
		String[] tournaments = tournamentIndex.getValue().toArray(new String[0]);
		if (tournaments.length > 0) {
			return tournaments[rg.nextInt(tournaments.length)];
		} else
			return null;
	}

	@SuppressWarnings("unchecked")
	protected Pair<String, String> selectTournamentPlayerPair(String tournament) throws SwiftException {
		stub.beginTxn();
		AddWinsSetCRDT<String> tournamentPlayers = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournament(tournament), true, AddWinsSetCRDT.class);
		stub.endTxn();
		String[] players = tournamentPlayers.getValue().toArray(new String[0]);
		if (players.length > 1) {
			int player1 = rg.nextInt(players.length);
			int player2 = player1;
			do {
				player2 = rg.nextInt(players.length);
			} while (player1 == player2);
			return new Pair<String, String>(players[player1], players[player2]);
		} else {
			if (logger.isLoggable(Level.WARNING)) {
				logger.warning("Tournament without enough players " + tournament);
			}
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	protected void addNewPlayersToTournament(final String[] playerNames, final String tournamentName) throws SwiftException {
		stub.beginTxn();
		int site = Integer.parseInt(tournamentName.split("_")[0]);
		for (String playerName : playerNames) {
			if (site != -1) {
				_addPlayer(site, playerName);
			}
			AddWinsSetCRDT<String> tournament = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournament(tournamentName), true, AddWinsSetCRDT.class);
			tournament.add(playerName);
			AddWinsSetCRDT<String> playerTournaments = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerTournaments(playerName), false, AddWinsSetCRDT.class);
			playerTournaments.add(tournamentName);
		}
		stub.endTxn();
	}
	protected boolean addPlayer(int site, String playerName) throws SwiftException {
		boolean result;
		stub.beginTxn();
		result = _addPlayer(site, playerName);
		stub.endTxn();
		if (fakeStub != null)
			fakeStub.request(surrogate, new CurrentClockRequest());
		return result;
	}

	@SuppressWarnings("unchecked")
	private boolean _addPlayer(final int site, final String playerName) throws SwiftException {
		boolean result = true;
		if (site == -1) {
			System.out.println("aqui");
		}
		// Add player to index
		AddWinsSetCRDT<String> playerIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerIndex(site + ""), true, AddWinsSetCRDT.class);
		result &= playerIndex.lookup(playerName);
		playerIndex.add(playerName);

		// Create Player lock
		CRDTIdentifier lockId = NamingScheme.forPlayerLock(playerName);
		EscrowableTokenCRDT lock = stub.get(lockId, true, EscrowableTokenCRDT.class);
		for (String site_i : allSites) {
			lock.transferOwnership(master, site_i, new LockReservation((String) site_i, lockId, ShareableLock.FORBID));
		}

		// Create player register
		LWWRegisterCRDT<Player> reg = (LWWRegisterCRDT<Player>) stub.get(NamingScheme.forPlayer(playerName), true, LWWRegisterCRDT.class);
		Player newPlayer = new Player(playerName, site);
		reg.set((Player) newPlayer.copy());
		playerIndex.add(playerName);

		// Create player's tournaments set
		stub.get(NamingScheme.forPlayerTournaments(playerName), true, AddWinsSetCRDT.class);
		if (site != -1 && !allSites[site - 1].equals(allSites[Integer.parseInt(playerName.split("_")[0]) - 1])) {
			System.out.println("aqui");
		}
		// System.err.println("Added Player " + playerName + " " + allSites[site
		// - 1]);
		return result;
	}
	// @SuppressWarnings("unchecked")
	// protected void removePlayer(final String playerName) throws
	// SwiftException {
	// try {
	// LWWRegisterCRDT<Player> reg =
	// stub.get(NamingScheme.forPlayer(playerName), false,
	// LWWRegisterCRDT.class);
	// Player player = reg.getValue();
	//
	// AddWinsSetCRDT<String> playerIndex =
	// stub.get(NamingScheme.forPlayerIndex(player.primarySite), true,
	// AddWinsSetCRDT.class);
	// playerIndex.remove(playerName);
	//
	// AddWinsSetCRDT<String> tournaments =
	// stub.get(NamingScheme.forPlayerTournaments(playerName), false,
	// AddWinsSetCRDT.class);
	// for (String tournamentName : tournaments.copy().getValue()) {
	// AddWinsSetCRDT<String> tournament =
	// stub.get(NamingScheme.forTournament(tournamentName), false,
	// AddWinsSetCRDT.class);
	// tournament.remove(playerName);
	// }
	// } catch (SwiftException e) {
	// logger.warning(e.getMessage());
	// }
	//
	// }

	protected boolean addTournament(final int tournamentSite, final String tournamentName, final int maxSize) throws SwiftException {
		boolean result;
		stub.beginTxn();
		result = _addTournament(tournamentSite, tournamentName, maxSize);
		stub.endTxn();
		if (fakeStub != null)
			fakeStub.request(surrogate, new CurrentClockRequest());
		return result;
	}
	@SuppressWarnings("unchecked")
	private boolean _addTournament(final int site, final String tournamentName, final int maxPlayers) throws SwiftException {
		boolean result = true;
		try {
			// Add tournament to index
			AddWinsSetCRDT<String> tournamentIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournamentIndex(site + ""), true, AddWinsSetCRDT.class);
			result &= tournamentIndex.lookup(tournamentName);
			tournamentIndex.add(tournamentName);

			stub.get(NamingScheme.forTournament(tournamentName), true, AddWinsSetCRDT.class);

			// Create tournament locks
			CRDTIdentifier lockId = NamingScheme.forTournamentLock(tournamentName);
			EscrowableTokenCRDT lock = stub.get(NamingScheme.forTournamentLock(tournamentName), true, EscrowableTokenCRDT.class);
			BoundedCounterAsResource tournamentCounter = stub.get(NamingScheme.forTournamentSize(tournamentName), true, BoundedCounterAsResource.class);
			if (site == -1) {
				for (String site_i : allSites) {
					lock.transferOwnership(master, site_i, new LockReservation((String) site_i, lockId, ShareableLock.FORBID));
					tournamentCounter.increment(maxPlayers / allSites.length, site_i);
				}
			} else {
				lock.transferOwnership(allSites[site - 1], allSites[site - 1], new LockReservation((String) allSites[site - 1], lockId, ShareableLock.FORBID));
				tournamentCounter.increment(maxPlayers, allSites[site - 1]);
				System.err.println("Added tournamnet " + tournamentName + " " + allSites[site - 1]);
			}
		} catch (SwiftException e) {
			if (logger.isLoggable(Level.WARNING)) {
				logger.warning(e.getMessage());
			}
		}
		return result;
	}
	@SuppressWarnings("unchecked")
	protected boolean removeTournament(final int site, final String tournamentName) throws SwiftException {
		boolean result = true;
		try {
			List<ResourceRequest<?>> resources = new LinkedList<>();
			resources.add(new LockReservation(siteId, NamingScheme.forTournamentLock(tournamentName), ShareableLock.ALLOW));
			stub.beginTxn(resources);

			// Read site's tournaments
			AddWinsSetCRDT<String> tournamentIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournamentIndex(site + ""), false, AddWinsSetCRDT.class);

			// Check tournament exists
			result &= tournamentIndex.lookup(tournamentName);
			tournamentIndex.remove(tournamentName);

			// Remove players from tournament
			AddWinsSetCRDT<String> TournamentMembers = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournament(tournamentName), false, AddWinsSetCRDT.class);
			for (String member : TournamentMembers.copy().getValue()) {
				TournamentMembers.remove(member);
				AddWinsSetCRDT<String> playerTeams = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerTournaments(member), false, AddWinsSetCRDT.class);
				playerTeams.remove(tournamentName);
			}

		} catch (SwiftException e) {
			if (logger.isLoggable(Level.WARNING)) {
				logger.warning(e.getMessage());
			}
		} finally {
			stub.endTxn();
			if (fakeStub != null)
				fakeStub.request(surrogate, new CurrentClockRequest());
		}
		return result;
	}
	@SuppressWarnings("unchecked")
	protected boolean enrollTournament(final int site, final String playerName, final String tournamentName) throws SwiftException {
		boolean result = true, exists = false, dec = false;
		List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
		try {
			resources.add(new LockReservation(siteId, NamingScheme.forPlayerLock(playerName), ShareableLock.FORBID));
			resources.add(new LockReservation(siteId, NamingScheme.forTournamentLock(tournamentName), ShareableLock.FORBID));
			resources.add(new CounterReservation(siteId, NamingScheme.forTournamentSize(tournamentName), 1));
			stub.beginTxn(resources);

			// Check tournament exists
			AddWinsSetCRDT<String> tournamentIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournamentIndex(site + ""), false, AddWinsSetCRDT.class);
			exists = tournamentIndex.lookup(tournamentName);

			// Add player to tournament
			AddWinsSetCRDT<String> tournamentSet = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournament(tournamentName), false, AddWinsSetCRDT.class);
			result &= !tournamentSet.lookup(playerName);
			tournamentSet.add(playerName);

			// Read players info
			stub.get(NamingScheme.forPlayer(playerName), false, LWWRegisterCRDT.class);

			// Update player's tournaments
			AddWinsSetCRDT<String> playerTournaments = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerTournaments(playerName), false, AddWinsSetCRDT.class);
			playerTournaments.add(tournamentName);
			BoundedCounterAsResource counter = stub.get(NamingScheme.forTournamentSize(tournamentName), false, BoundedCounterAsResource.class);
			dec = counter.decrement(1, siteId);
			if (!dec) {
				System.err.println("DIDNT decrement " + counter);
			}

			result = result && exists && dec;
			if (logger.isLoggable(Level.WARNING) && result == false) {
				if (!dec)
					logger.warning("Decrement failed!!! " + counter);
				if (!exists)
					logger.warning("Tournament does not exist in index " + tournamentName);
			}
		} catch (SwiftException e) {
			if (logger.isLoggable(Level.WARNING)) {
				logger.warning(resources + e.getMessage());
			}
		} finally {
			stub.endTxn();
			if (fakeStub != null)
				fakeStub.request(surrogate, new CurrentClockRequest());
		}
		return result;
	}
	@SuppressWarnings("unchecked")
	protected boolean disenrollTournament(String player, String tournamentName) throws SwiftException {
		boolean result = true;
		try {
			stub.beginTxn();

			// Get tournament
			AddWinsSetCRDT<String> tournamentSet = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forTournament(tournamentName), false, AddWinsSetCRDT.class);

			// Get player's tournaments
			AddWinsSetCRDT<String> playerTournaments = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerTournaments(player), false, AddWinsSetCRDT.class);

			result &= tournamentSet.lookup(tournamentName);
			tournamentSet.remove(player);
			playerTournaments.remove(tournamentName);
			BoundedCounterAsResource counter = stub.get(NamingScheme.forTournamentSize(tournamentName), false, BoundedCounterAsResource.class);
			counter.increment(1, siteId);
		} catch (SwiftException e) {
			if (logger.isLoggable(Level.WARNING)) {
				logger.warning(e.getMessage());
			}
		} finally {
			stub.endTxn();
			if (fakeStub != null)
				fakeStub.request(surrogate, new CurrentClockRequest());
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	protected boolean doMatch(String matchId, String tournamentName, String player1, String player2) throws SwiftException {
		boolean result = true;
		try {
			List<ResourceRequest<?>> resources = new LinkedList<>();
			resources.add(new LockReservation(siteId, NamingScheme.forTournamentLock(tournamentName), ShareableLock.FORBID));
			resources.add(new LockReservation(siteId, NamingScheme.forPlayerLock(player1), ShareableLock.FORBID));
			resources.add(new LockReservation(siteId, NamingScheme.forPlayerLock(player2), ShareableLock.FORBID));

			stub.beginTxn(resources);
			AddWinsSetCRDT<String> player1Tournaments = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerTournaments(player1), false, AddWinsSetCRDT.class);
			AddWinsSetCRDT<String> player2Tournaments = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerTournaments(player2), false, AddWinsSetCRDT.class);

			// Check that both players are enrolled in the same tournament
			if (player1Tournaments.lookup(tournamentName) && player2Tournaments.lookup(tournamentName)) {
				AddWinsSetCRDT<Match> matchHistory = (AddWinsSetCRDT<Match>) stub.get(NamingScheme.forMatchHistory(tournamentName), true, AddWinsSetCRDT.class);
				Match match = new Match(matchId, player1, player2);
				matchHistory.add(match);
			} else {
				result = false;
				if (logger.isLoggable(Level.INFO))
					logger.info("One of the players is not enrolled in the tournamnet " + tournamentName);
			}
		} catch (SwiftException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.getMessage());
		} finally {
			stub.endTxn();
			if (fakeStub != null)
				fakeStub.request(surrogate, new CurrentClockRequest());
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	protected void viewStatus(String tournamentName) throws SwiftException {
		try {
			stub.beginTxn();
			stub.get(NamingScheme.forTournament(tournamentName), false, AddWinsSetCRDT.class);
		} catch (SwiftException e) {
			logger.warning(e.getMessage());
		} finally {
			if (fakeStub != null)
				fakeStub.request(surrogate, new CurrentClockRequest());
			stub.endTxn();
		}
	}
}
