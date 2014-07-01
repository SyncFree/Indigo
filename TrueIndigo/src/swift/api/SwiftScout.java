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
package swift.api;

import static sys.utils.NotImplemented.NotImplemented;

/**
 * Shared Swift scout that allows a single or multiple open concurrent sessions.
 * 
 * @author mzawirski
 */
public interface SwiftScout {
	/**
	 * Creates a new session with the shared Swift scout. Note that session do
	 * not introduce extra overhead and do not need to be explicitly closed.
	 * 
	 * @param sessionId
	 * @return a new session client, associated with this shared instance
	 * @throws UnsupportedOperationException
	 *             when scout does not support more sessions
	 */
	SwiftSession newSession(final String sessionId);

	/**
	 * Stops the scout, which renders it unusable after this call returns.
	 * 
	 * @param waitForCommit
	 *            when true, this call blocks until all locally committed
	 *            transactions commit globally in the store
	 * 
	 */
	void stop(boolean waitForCommit);

	/**
	 * Prints and resets caching statistics.
	 */
	default void printAndResetCacheStats() {
		throw NotImplemented;
	}

	/**
	 * Returns scout id;
	 * 
	 * @return
	 */
	public String getScoutId();

}
