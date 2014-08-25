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

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import swift.crdt.AddWinsSetCRDT;
import swift.crdt.BoundedCounterAsResource;
import swift.crdt.EscrowableTokenCRDT;
import swift.crdt.LWWRegisterCRDT;
import swift.crdt.ShareableLock;
import swift.exceptions.SwiftException;
import swift.indigo.CounterReservation;
import swift.indigo.Indigo;
import swift.indigo.LockReservation;
import swift.indigo.ResourceRequest;
import swift.utils.Pair;

import com.thoughtworks.xstream.core.util.Base64Encoder;

// implements the ad service functionality

public class TournamentServiceOps {

	private static Logger logger = Logger.getLogger("indigo.tournament");

	private Indigo stub;
	final private String siteId;

	private Random rg;

	public TournamentServiceOps(Indigo stub, String siteId) {
		this.stub = stub;
		this.siteId = siteId;
		this.rg = new Random();
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
		AddWinsSetCRDT<String> playerIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerIndex(site), true,
				AddWinsSetCRDT.class);
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
		AddWinsSetCRDT<String> tournamentIndex = (AddWinsSetCRDT<String>) stub.get(
				NamingScheme.forTournamentIndex(site), true, AddWinsSetCRDT.class);
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
		AddWinsSetCRDT<String> tournamentPlayers = (AddWinsSetCRDT<String>) stub.get(
				NamingScheme.forTournament(tournament), true, AddWinsSetCRDT.class);
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
			logger.info("Tournament without enough players");
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	protected void addNewPlayersToTournament(final String[] playerNames, final int tournamentSite,
			final String tournamentName) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<>();

		// If site is -1, player already exists in other site (TODO: check this)
		if (tournamentSite != -1) {
			for (String playerName : playerNames) {
				resources.add(new LockReservation(siteId, NamingScheme.forPlayerLock(playerName), ShareableLock
						.getDefault()));
			}

		}
		stub.beginTxn(resources);

		for (String playerName : playerNames) {
			_addPlayer(tournamentSite, playerName);
			AddWinsSetCRDT<String> tournament = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forTournament(tournamentName), true, AddWinsSetCRDT.class);
			tournament.add(playerName);
			AddWinsSetCRDT<String> playerTournaments = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forPlayerTournaments(playerName), false, AddWinsSetCRDT.class);
			playerTournaments.add(tournamentName);

		}
		stub.endTxn();
	}
	protected void addPlayer(int site, String playerName) throws SwiftException {
		List<ResourceRequest<?>> resources = new LinkedList<>();
		resources.add(new LockReservation(siteId, NamingScheme.forPlayerLock(playerName), ShareableLock.getDefault()));
		stub.beginTxn(resources);
		_addPlayer(site, playerName);
		stub.endTxn();
	}

	@SuppressWarnings("unchecked")
	private void _addPlayer(final int site, final String playerName) throws SwiftException {

		// Create Player lock
		stub.get(NamingScheme.forPlayerLock(playerName), false, EscrowableTokenCRDT.class);

		AddWinsSetCRDT<String> playerIndex = (AddWinsSetCRDT<String>) stub.get(NamingScheme.forPlayerIndex(site), true,
				AddWinsSetCRDT.class);
		playerIndex.add(playerName);

		LWWRegisterCRDT<Player> reg = (LWWRegisterCRDT<Player>) stub.get(NamingScheme.forPlayer(playerName), true,
				LWWRegisterCRDT.class);
		Player newPlayer = new Player(playerName, site);
		reg.set((Player) newPlayer.copy());
		playerIndex.add(playerName);

		// Create player tournaments set
		stub.get(NamingScheme.forPlayerTournaments(playerName), true, AddWinsSetCRDT.class);
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

	protected void addTournament(final int tournamentSite, final String tournamentName, final int maxSize)
			throws SwiftException {

		List<ResourceRequest<?>> resources = new LinkedList<>();
		resources.add(new LockReservation(siteId, NamingScheme.forTournamentLock(tournamentName), ShareableLock
				.getDefault()));
		resources.add(new CounterReservation(siteId, NamingScheme.forTournamentSize(tournamentName), 0));
		stub.beginTxn(resources);
		_addTournament(tournamentSite, tournamentName, maxSize);
		stub.endTxn();
	}
	@SuppressWarnings("unchecked")
	private void _addTournament(final int tournamentSite, final String tournamentName, final int maxSize)
			throws SwiftException {
		try {
			AddWinsSetCRDT<String> tournamnetIndex = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forTournamentIndex(tournamentSite), true, AddWinsSetCRDT.class);
			stub.get(NamingScheme.forTournament(tournamentName), true, AddWinsSetCRDT.class);
			BoundedCounterAsResource tournamentCounter = stub.get(NamingScheme.forTournamentSize(tournamentName),
					false, BoundedCounterAsResource.class);
			tournamentCounter.increment(maxSize, siteId);

			tournamnetIndex.add(tournamentName);
			logger.info("Created tournament: " + tournamentName + " TOTAL: " + tournamnetIndex.size());
		} catch (SwiftException e) {
			logger.warning(e.getMessage());
		}
	}
	@SuppressWarnings("unchecked")
	protected void removeTournament(final int index, final String tournamentName) throws SwiftException {
		try {

			List<ResourceRequest<?>> resources = new LinkedList<>();
			resources.add(new LockReservation(siteId, NamingScheme.forTournamentLock(tournamentName),
					ShareableLock.ALLOW));

			stub.beginTxn(resources);
			AddWinsSetCRDT<String> tournamentIndex = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forTournamentIndex(index), false, AddWinsSetCRDT.class);

			tournamentIndex.remove(tournamentName);

			AddWinsSetCRDT<String> TournamentMembers = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forTournament(tournamentName), false, AddWinsSetCRDT.class);

			for (String member : TournamentMembers.copy().getValue()) {
				TournamentMembers.remove(member);
				AddWinsSetCRDT<String> playerTeams = (AddWinsSetCRDT<String>) stub.get(
						NamingScheme.forPlayerTournaments(member), false, AddWinsSetCRDT.class);
				playerTeams.remove(tournamentName);
			}
			logger.info("Tournament emptied and remvoed: " + tournamentName);
		} catch (SwiftException e) {
			logger.warning(e.getMessage());
		} finally {
			stub.endTxn();
		}

	}

	@SuppressWarnings("unchecked")
	protected void enrollTournament(String playerName, String tournamentName, int maxSize) throws SwiftException {
		try {

			List<ResourceRequest<?>> resources = new LinkedList<ResourceRequest<?>>();
			resources.add(new LockReservation(siteId, NamingScheme.forPlayerLock(playerName), ShareableLock.FORBID));
			resources.add(new LockReservation(siteId, NamingScheme.forTournamentLock(tournamentName),
					ShareableLock.FORBID));
			resources.add(new CounterReservation(siteId, NamingScheme.forTournamentSize(tournamentName), 1));

			stub.beginTxn(resources);

			AddWinsSetCRDT<String> tournamentSet = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forTournament(tournamentName), false, AddWinsSetCRDT.class);

			stub.get(NamingScheme.forPlayer(playerName), false, LWWRegisterCRDT.class);
			tournamentSet.add(playerName);
			AddWinsSetCRDT<String> playerTournaments = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forPlayerTournaments(playerName), false, AddWinsSetCRDT.class);
			playerTournaments.add(tournamentName);

			BoundedCounterAsResource counter = stub.get(NamingScheme.forTournamentSize(tournamentName), false,
					BoundedCounterAsResource.class);
			counter.decrement(1, siteId);
		} catch (SwiftException e) {
			logger.warning(e.getMessage());
		} finally {
			stub.endTxn();
		}
	}

	@SuppressWarnings("unchecked")
	protected void disenrollTournament(String player, String tournamentName) throws SwiftException {
		try {
			stub.beginTxn();
			AddWinsSetCRDT<String> tournamentSet = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forTournament(tournamentName), false, AddWinsSetCRDT.class);
			AddWinsSetCRDT<String> playerTournaments = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forPlayerTournaments(player), false, AddWinsSetCRDT.class);
			tournamentSet.remove(player);
			playerTournaments.remove(tournamentName);

			// TODO: Explicitly updating the counter
			BoundedCounterAsResource counter = stub.get(NamingScheme.forTournamentSize(tournamentName), false,
					BoundedCounterAsResource.class);
			counter.increment(1, siteId);
		} catch (SwiftException e) {
			logger.warning(e.getMessage());
		} finally {
			stub.endTxn();
		}

	}

	@SuppressWarnings("unchecked")
	protected void doMatch(String matchId, String tournamentName, String player1, String player2) throws SwiftException {
		try {
			List<ResourceRequest<?>> resources = new LinkedList<>();
			resources.add(new LockReservation(siteId, NamingScheme.forTournamentLock(tournamentName),
					ShareableLock.ALLOW));
			resources.add(new LockReservation(siteId, NamingScheme.forPlayerLock(player1), ShareableLock.ALLOW));
			resources.add(new LockReservation(siteId, NamingScheme.forPlayerLock(player2), ShareableLock.ALLOW));

			stub.beginTxn(resources);
			AddWinsSetCRDT<String> player1Tournaments = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forPlayerTournaments(player1), false, AddWinsSetCRDT.class);

			AddWinsSetCRDT<String> player2Tournaments = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forPlayerTournaments(player2), false, AddWinsSetCRDT.class);

			if (player1Tournaments.lookup(tournamentName) && player2Tournaments.lookup(tournamentName)) {
				AddWinsSetCRDT<Match> matchHistory = (AddWinsSetCRDT<Match>) stub.get(NamingScheme.forMatchHistory(),
						true, AddWinsSetCRDT.class);
				Match match = new Match(matchId, player1, player2);
				matchHistory.add(match);
			} else {
				logger.info("One of the players is not enrolled in the tournamnet " + tournamentName);
			}
		} catch (SwiftException e) {
			logger.warning(e.getMessage());
		} finally {
			stub.endTxn();
		}
	}

	@SuppressWarnings("unchecked")
	protected void viewStatus(String tournamentName) throws SwiftException {
		try {
			stub.beginTxn();
			AddWinsSetCRDT<String> tournament = (AddWinsSetCRDT<String>) stub.get(
					NamingScheme.forTournament(tournamentName), false, AddWinsSetCRDT.class);

			if (tournament != null)
				for (String player : tournament.getValue()) {
					stub.get(NamingScheme.forPlayer(player), false, LWWRegisterCRDT.class);
				}
		} catch (SwiftException e) {
			logger.warning(e.getMessage());
		} finally {
			stub.endTxn();
		}
	}

}
