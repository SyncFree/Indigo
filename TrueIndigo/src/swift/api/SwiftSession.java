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

import swift.client.SwiftClient;
import swift.crdt.core.CachePolicy;
import swift.exceptions.NetworkException;
import swift.exceptions.SwiftException;

/**
 * API for the Swift system, a client session that can issue transactions. A
 * session is normally attached to a scout ({@link SwiftScout}). See
 * {@link SwiftClient} to learn how to start a scout and session. Note that all
 * "session guarantees" apply to this unit of session.
 * 
 * @author annettebieniusa, mzawirski
 * @see SwiftClient
 */
public interface SwiftSession {
	/**
	 * Starts a new transaction, observing the results of locally committed
	 * transactions in this session, and some external transactions committed to
	 * the store, depending on cache and isolation options.
	 * 
	 * @param isolationLevel
	 *            isolation level defining consistency guarantees for
	 *            transaction reads
	 * @param cachePolicy
	 *            cache policy for the new transaction
	 * @param readOnly
	 *            when true, the transaction cannot generate any updates or
	 *            create objects; recommended for read-only transactions for
	 *            better performance
	 * @return TxnHandle for the new transaction
	 * @throws IllegalStateException
	 *             when another transaction is pending in the system, or the
	 *             client is already stopped
	 * @throws NetworkException
	 *             when strict cachePolicy is selected and the store does not
	 *             reply
	 */
	TxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cachePolicy, boolean readOnly)
			throws NetworkException, SwiftException;

	// WISHME: in order to support disconnected operations w/client partial
	// replication, extend API to start transaction and prefetch/update some
	// objects and to switch IsolationLevel/CachePolicy after stating the
	// transaction.

	/**
	 * Stops the underlying scout, which renders it unusable after this call
	 * returns. Careful, it may also affect other sessions of this scout.
	 * 
	 * @param waitForCommit
	 *            when true, this call blocks until all locally committed
	 *            transactions commit globally in the store
	 */
	void stopScout(boolean waitForCommit);

	// TODO: change to stopSession() and count opened sessions?

	/**
	 * @return session identifier
	 */
	String getSessionId();

	/**
	 * @return scout associated with this session
	 */
	SwiftScout getScout();

	void printStatistics();
}
