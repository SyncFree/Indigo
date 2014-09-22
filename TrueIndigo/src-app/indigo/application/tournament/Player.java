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
import swift.crdt.core.Copyable;

public class Player implements Copyable {
	String name;
	String primarySite;

	CRDTIdentifier viewCount;

	/** DO NOT USE: Empty constructor needed for Kryo */
	public Player() {
	}

	public Player(final String name, final String primarySite) {
		this.name = name;
		this.primarySite = primarySite;
	}

	@Override
	public Object copy() {
		Player copyObj = new Player(name, primarySite);
		return copyObj;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(name);
		sb.append("NAME: ");
		sb.append(name);
		return sb.toString();
	}

}
