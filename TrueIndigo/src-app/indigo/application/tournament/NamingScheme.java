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

import swift.api.CRDTIdentifier;

/**
 * Provides methods for generating CRDT Identifiers based on the class and type
 * of object.
 * 
 * @author annettebieniusa
 * 
 */

public class NamingScheme {

	public static CRDTIdentifier forPlayer(String playerName) {
		return new CRDTIdentifier("player", playerName);
	}

	public static CRDTIdentifier forPlayerTournaments(String playerName) {
		return new CRDTIdentifier("player_tournaments", playerName);
	}

	public static CRDTIdentifier forTournament(String tournamentName) {
		return new CRDTIdentifier("tournament", tournamentName);
	}

	public static CRDTIdentifier fotMatches(String matchId) {
		return new CRDTIdentifier("matches", matchId);
	}

	public static CRDTIdentifier forMatchHistory() {
		return new CRDTIdentifier("indexes", "matches");
	}

	public static CRDTIdentifier forPlayerIndex(int site) {
		return new CRDTIdentifier("indexes", "players_" + site);
	}

	public static CRDTIdentifier forTournamentIndex(int site) {
		return new CRDTIdentifier("indexes", "tournaments_" + site);
	}

	public static CRDTIdentifier forTournamentSize(String tournamentName) {
		return new CRDTIdentifier("tournament_counter", tournamentName);
	}

	public static CRDTIdentifier forTournamentLock(String tournamentName) {
		return new CRDTIdentifier("tournament_lock", tournamentName);
	}

	public static CRDTIdentifier forPlayerLock(String playerName) {
		return new CRDTIdentifier("player_lock", playerName);
	}

}
