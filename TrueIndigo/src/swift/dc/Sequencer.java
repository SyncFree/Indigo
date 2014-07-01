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
package swift.dc;

import static swift.clocks.CausalityClock.CMP_CLOCK.CMP_DOMINATES;
import static swift.clocks.CausalityClock.CMP_CLOCK.CMP_EQUALS;
import static sys.Context.Networking;
import static sys.Context.Sys;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.Timestamp;
import swift.proto.CommitTimestampReply;
import swift.proto.CommitTimestampReply.CommitTSStatus;
import swift.proto.CommitTimestampRequest;
import swift.proto.CurrentClockReply;
import swift.proto.CurrentClockRequest;
import swift.proto.GenerateTimestampReply;
import swift.proto.GenerateTimestampRequest;
import swift.proto.SequencerProtocol;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Args;
import sys.utils.FifoQueue;
import sys.utils.Tasks;
/**
 * 
 * @author nmp,smduarte
 * 
 */
public class Sequencer implements SequencerProtocol {
	private static Logger logger = Logger.getLogger(Sequencer.class.getName());

	final ConcurrentHashMap<String, FifoQueue<CommitTimestampRequest>> fifoQueues;
	final ConcurrentHashMap<Timestamp, GenerateTimestampRequest> pendingTimestampRequests;
	final ConcurrentHashMap<Timestamp, Long> pendingTimestamps;

	public final Service stub;
	public final String siteId;
	public final Clocks clocks;

	protected Sequencer() {

		this.siteId = Args.valueOf("-siteId", "X");

		this.fifoQueues = new ConcurrentHashMap<String, FifoQueue<CommitTimestampRequest>>();
		this.pendingTimestampRequests = new ConcurrentHashMap<Timestamp, GenerateTimestampRequest>();
		this.pendingTimestamps = new ConcurrentHashMap<Timestamp, Long>();

		Endpoint localEndpoint = Networking.resolve(Args.valueOf("-url", Defaults.SEQUENCER_URL));
		this.stub = Networking.bind(localEndpoint, this);

		this.clocks = new Clocks("Sequencer");

		if (logger.isLoggable(Level.INFO)) {
			logger.info("Sequencer ready... @ " + localEndpoint);
		}

		Tasks.every(1.0, () -> {
			cleanExpiredTimestamps();
		});

		Tasks.every(0.1, () -> {
			cleanPendingTimestampRequests();
		});
	}

	protected void start() {
		System.err.println("Sequencer ready...");
	}

	private void cleanExpiredTimestamps() {

		long now = Sys.timeMillis();
		Set<Timestamp> expired = new HashSet<Timestamp>();
		pendingTimestamps.forEach((ts, deadline) -> {
			if (now > deadline) {
				expired.add(ts);
				clocks.record(ts, clocks.currentClock, clocks.notUsed);
			}
		});
		pendingTimestamps.keySet().removeAll(expired);
	}

	private void cleanPendingTimestampRequests() {

		Set<Timestamp> processed = new HashSet<Timestamp>();
		pendingTimestampRequests.forEach((ts, req) -> {
			if (onReceive(req.getSource(), req))
				processed.add(ts);
		});
		pendingTimestampRequests.keySet().removeAll(processed);
	}

	@Override
	public void onReceive(final Envelope src, final CurrentClockRequest r) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("sequencer: CurrentClockRequest: " + r);
		}
		src.reply(new CurrentClockReply(clocks.currentClockCopy()));
	}

	@Override
	public boolean onReceive(final Envelope src, final GenerateTimestampRequest r) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("sequencer: GenerateTimestampRequest: " + r);
		}

		Timestamp cTS = r.getCltTimestamp();

		CausalityClock cltClock = clocks.clientClockCopy();

		long last = cltClock.getLatestCounter(cTS.getIdentifier());
		if (cltClock.includes(cTS)) {
			src.reply(new GenerateTimestampReply(last));
			return true;
		}

		CMP_CLOCK cmp = clocks.cmp(clocks.currentClock, r.getDependencyClk());

		if (cTS.getCounter() == (last + 1L) && cmp.is(CMP_EQUALS, CMP_DOMINATES)) {
			Timestamp ts = clocks.newTimestamp();
			pendingTimestamps.put(ts, Sys.timeMillis() + Defaults.DEFAULT_TRXIDTIME * 2);
			src.reply(new GenerateTimestampReply(ts, last));
			return true;
		} else {
			r.setSource(src);
			pendingTimestampRequests.put(cTS, r);
			return false;
		}
	}
	private void doCommit(CommitTimestampRequest req) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("sequencer: CommitTimestampRequest:" + req.getTimestamp());
		}
		System.err.println("COMMIT:" + req.getTimestamp() + "/" + req.getCltTimestamp());

		if (!clocks.record(req.getCltTimestamp(), clocks.clientClock))
			req.getSource().reply(new CommitTimestampReply(CommitTSStatus.FAILED, clocks.currentClockCopy()));

		Timestamp ts = req.getTimestamp();
		pendingTimestamps.remove(ts);
		clocks.record(ts, clocks.currentClock);

		req.getSource().reply(new CommitTimestampReply(CommitTSStatus.OK, clocks.currentClockCopy()));
	}

	private FifoQueue<CommitTimestampRequest> queueFor(final String clientId) {
		FifoQueue<CommitTimestampRequest> res = fifoQueues.get(clientId), nq;
		if (res == null) {
			res = fifoQueues.putIfAbsent(clientId, nq = new FifoQueue<CommitTimestampRequest>(clientId) {
				public void process(CommitTimestampRequest r) {
					doCommit(r);
				}
			});
			if (res == null)
				res = nq;
		}
		return res;
	}

	@Override
	public void onReceive(final Envelope src, final CommitTimestampRequest req) {
		req.setSource(src);
		Timestamp ts = req.getTimestamp();
		queueFor(ts.getIdentifier()).offer(ts.getCounter(), req);
	}

	public static void main(String[] args) {
		Args.use(args);

		new Sequencer().start();
	}

}