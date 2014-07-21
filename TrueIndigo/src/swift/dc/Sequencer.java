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

import static sys.Context.Networking;
import static sys.Context.Sys;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

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
import sys.utils.Tasks;

/**
 * 
 * @author nmp,smduarte
 * 
 */
public class Sequencer implements SequencerProtocol {
    private static Logger Log = Logger.getLogger(Sequencer.class.getName());

    final ConcurrentHashMap<Timestamp, Long> pendingTimestamps;

    public final Service stub;
    public final String siteId;
    public final Clocks clocks;

    final FifoQueues fifo;

    protected Sequencer() {

        this.siteId = Args.valueOf("-siteId", "X");

        this.fifo = new FifoQueues();
        this.pendingTimestamps = new ConcurrentHashMap<>();

        Endpoint localEndpoint = Networking.resolve(Args.valueOf("-url", Defaults.SEQUENCER_URL));
        this.stub = Networking.bind(localEndpoint, this);

        this.clocks = new Clocks("Sequencer");

        if (Log.isLoggable(Level.INFO)) {
            Log.info("Sequencer ready... @ " + localEndpoint);
        }

        Tasks.every(1.0, () -> {
            cleanExpiredTimestamps();
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

    @Override
    public void onReceive(final Envelope src, final CurrentClockRequest r) {
        // if (Log.isLoggable(Level.INFO)) {
        // Log.info("sequencer: CurrentClockRequest: " + r);
        // }
        src.reply(new CurrentClockReply(clocks.currentClockCopy()));
    }

    @Override
    public void onReceive(final Envelope src, final GenerateTimestampRequest req) {
        if (Log.isLoggable(Level.INFO)) {
            Log.info("pre sequencer: GenerateTimestampRequest: " + req);
        }

        if (clocks.record(req.getCltTimestamp(), clocks.clientClock)) {
            req.setSource(src);
            fifo.queue4GenTS(req.getCltTimestamp().getIdentifier()).enqueue(req.getCltTimestamp().getCounter(), req,
                    (GenerateTimestampRequest i) -> {
                        if (Log.isLoggable(Level.INFO)) {
                            Log.info("do sequencer: GenerateTimestampRequest: " + i);
                        }
                        Timestamp iTS = clocks.newTimestamp();
                        pendingTimestamps.put(iTS, Sys.timeMillis() + Defaults.DEFAULT_TRXIDTIME * 2);
                        i.getSource().reply(new GenerateTimestampReply(iTS, i.getCltTimestamp()));
                    });
        } else {
            if (Log.isLoggable(Level.INFO)) {
                Log.info("do sequencer: Duplicate GenerateTimestampRequest: " + req);
            }
            src.reply(new GenerateTimestampReply(req.getCltTimestamp()));
        }
    }

    @Override
    public void onReceive(final Envelope src, final CommitTimestampRequest req) {
        req.rtClock = System.currentTimeMillis();
        Timestamp ts = req.getTimestamp();
        if (Log.isLoggable(Level.INFO)) {
            Log.info("sequencer: CommitTimestampRequest:" + req.getTimestamp());
        }
        if (clocks.record(ts, clocks.currentClock)) {
            pendingTimestamps.remove(ts);
            src.reply(new CommitTimestampReply(CommitTSStatus.OK, clocks.currentClockCopy()));
        } else
            src.reply(new CommitTimestampReply(CommitTSStatus.FAILED, clocks.currentClockCopy()));
    }

    // @Override
    // public void onReceive(final Envelope src, final CommitTimestampRequest
    // req) {
    // req.rtClock = System.currentTimeMillis();
    //
    // req.setSource(src);
    // Timestamp ts = req.getTimestamp();
    // fifo.queue4CommitTS(ts.getIdentifier()).enqueue(ts.getCounter(), req,
    // (CommitTimestampRequest i) -> {
    // if (Log.isLoggable(Level.INFO)) {
    // Log.info("sequencer: CommitTimestampRequest:" + req.getTimestamp());
    // }
    // System.err.println(siteId + " @@@@ " + i.getTimestamp() + " waited:" +
    // i.latency() + " ms");
    // Timestamp iTS = i.getTimestamp();
    // if (clocks.record(iTS, clocks.currentClock)) {
    // pendingTimestamps.remove(iTS);
    // i.getSource().reply(new CommitTimestampReply(CommitTSStatus.OK,
    // clocks.currentClockCopy()));
    // } else
    // i.getSource().reply(new CommitTimestampReply(CommitTSStatus.FAILED,
    // clocks.currentClockCopy()));
    // });
    // }

    public static void main(String[] args) {
        Args.use(args);

        new Sequencer().start();
    }

}