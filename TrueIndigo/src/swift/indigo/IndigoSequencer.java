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
package swift.indigo;

import java.util.logging.Level;
import java.util.logging.Logger;

import swift.dc.Sequencer;
import swift.indigo.proto.AcquireLocksRequest;
import swift.indigo.proto.CreateLocksRequest;
import swift.indigo.proto.IndigoProtocol;
import swift.indigo.proto.ReleaseLocksRequest;
import swift.indigo.proto.TransferReservationRequest;
import swift.proto.CommitTimestampRequest;
import sys.net.api.Envelope;
import sys.utils.Args;

/**
 * 
 * @author smduarte
 * 
 */
public class IndigoSequencer extends Sequencer implements IndigoProtocol {
	private static Logger logger = Logger.getLogger(IndigoSequencer.class.getName());

	IndigoLockManager lockManager;

	IndigoSequencer() {
	}

	@Override
	public void start() {
		lockManager = new IndigoLockManager(this);

		System.err.println("Indigo sequencer ready...");
	}

	/**
	 * @param conn
	 *            connection such that the remote end implements
	 *            {@link CommitTSReplyHandler} and expects {@link CommitTSReply}
	 * @param request
	 *            request to serve
	 */
	@Override
	public void onReceive(final Envelope src, final CommitTimestampRequest req) {
		if (lockManager.releaseLocks(req.getCltTimestamp())) {
			super.onReceive(src, req);
		} else {
			// TODO should fail, somehow...
		}
	}

	public void onReceive(Envelope src, final AcquireLocksRequest request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got AcquireLocksRequest:" + request);

		if (request.isLocalRequest())
			src.reply(lockManager.acquireLocks(request));
		else
			lockManager.updateOwnershipForRemoteDC(request);
	}

	public void onReceive(Envelope src, final ReleaseLocksRequest request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got ReleaseLocksRequest:" + request);

		lockManager.releaseLocks(request.cltTimestamp());
	}

	public void onReceive(Envelope src, final TransferReservationRequest request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got ReleaseLocksRequest:" + request);

		lockManager.transferReservation(request);
	}

	public void onReceive(Envelope src, final CreateLocksRequest request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got CreateLocksRequest:" + request);

		if (request.hasLocks()) {
			lockManager.createLocks(request.requesterId(), request.locks());
		}
		if (request.hasCounters()) {
			lockManager.createCounters(request.requesterId(), request.counters());
		}
	}

	public static void main(String[] args) {
		Args.use(args);

		new IndigoSequencer().start();
	}
}
