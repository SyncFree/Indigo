package swift.dc;

import static swift.clocks.CausalityClock.CMP_CLOCK.CMP_CONCURRENT;
import static swift.clocks.CausalityClock.CMP_CLOCK.CMP_ISDOMINATED;
import static swift.dc.Defaults.SERVER_URL;
import static swift.dc.Defaults.SERVER_URL4SEQUENCERS;
import static sys.Context.Networking;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.proto.CommitTimestampReply;
import swift.proto.CommitTimestampRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesRequest;
import swift.proto.CurrentClockReply;
import swift.proto.CurrentClockRequest;
import swift.proto.DHTExecCRDT;
import swift.proto.FetchObjectVersionReply;
import swift.proto.FetchObjectVersionReply.FetchStatus;
import swift.proto.FetchObjectVersionRequest;
import swift.proto.GenerateTimestampReply;
import swift.proto.GenerateTimestampRequest;
import swift.proto.RemoteCommitUpdatesReply;
import swift.proto.RemoteCommitUpdatesRequest;
import swift.proto.SurrogateProtocol;
import swift.pubsub.RemoteSwiftSubscriber;
import swift.pubsub.SurrogatePubSubService;
import swift.pubsub.UpdateNotification;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Args;
import sys.utils.Tasks;

/**
 * Class to handle the requests from clients.
 * 
 * @author preguica, smduarte
 */
public class Server implements SurrogateProtocol {
	static Logger Log = Logger.getLogger(Server.class.getName());

	public final String siteId;
	public final String serverId;

	public final Clocks clocks;

	public final DataServer dataServer;

	public final Service endpoint4clts;
	public final Service endpoint4servers;

	public final Endpoint sequencer;

	public final SurrogatePubSubService suPubSub;

	final ThreadPoolExecutor crdtExecutor;
	final Executor generalExecutor = Executors.newCachedThreadPool();

	final KStabilizer kStability;
	final FifoQueues fifo;

	protected Server() {

		this.siteId = Args.valueOf("-siteId", "X");
		this.serverId = "s" + System.nanoTime();

		this.clocks = new Clocks("server");

		this.endpoint4clts = Networking.bind(Networking.resolve(Args.valueOf("-url", SERVER_URL)), this);
		this.endpoint4servers = Networking.bind(Networking.resolve(Args.valueOf("-url4seq", SERVER_URL4SEQUENCERS)), this);

		this.sequencer = Networking.resolve(Args.valueOf("-sequencer", ""), Defaults.SEQUENCER_URL);

		this.dataServer = new DataServer(this);

		this.kStability = new KStabilizer(this);
		this.fifo = new FifoQueues();

		this.suPubSub = new SurrogatePubSubService(generalExecutor, this);

		ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(512);
		crdtExecutor = new ThreadPoolExecutor(4, 8, 3, TimeUnit.SECONDS, workQueue);
		crdtExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		Tasks.every(0.05, () -> {
			endpoint4servers.asyncRequest(sequencer, new CurrentClockRequest(serverId), (CurrentClockReply r) -> {
				this.updateCurrentClock(r.getClock());
			});
		});

		if (Log.isLoggable(Level.INFO)) {
			Log.info("Server ready...");
		}
	}

	protected void updateCurrentClock(CausalityClock newClock) {
		if (clocks.updateCurrentClock(newClock) == CausalityClock.CMP_CLOCK.CMP_ISDOMINATED)
			this.kStability.checkBlockedTransactions();
	}

	<V extends CRDT<V>> boolean execTxnCRDTOps(final CommitUpdatesRequest req, final CausalityClock clock) {

		final List<CRDTObjectUpdatesGroup<?>> ops = req.getObjectUpdateGroups();

		final AtomicBoolean txnOK = new AtomicBoolean(true);
		if (ops.size() > 2) { // do multiple execCRDTs in parallel
			final Semaphore s = new Semaphore(0);
			ops.forEach(i -> {
				crdtExecutor.execute(() -> {
					try {
						DHTExecCRDT execReq = new DHTExecCRDT(i, req.getCltTimestamp(), null, clock);
						ExecCRDTResult res = dataServer.execCRDT(execReq);
						txnOK.compareAndSet(true, res.isResult());
					} finally {
						s.release();
					}
				});
			});
			s.acquireUninterruptibly(ops.size());
		} else {
			ops.forEach(i -> {
				DHTExecCRDT execReq = new DHTExecCRDT(i, req.getCltTimestamp(), null, clock);
				ExecCRDTResult res = dataServer.execCRDT(execReq);
				txnOK.compareAndSet(true, res.isResult());
			});
		};
		return txnOK.get();
	}
	/**
	 * Return null if CRDT does not exist
	 * 
	 * @param subscribe
	 *            Subscription type
	 */
	public ManagedCRDT<?> getCRDT(CRDTIdentifier id, CausalityClock clk, String clientId) {
		return dataServer.getCRDT(id, clk, clientId, suPubSub.isSubscribed(id));
	}

	@Override
	public void onReceive(Envelope src, FetchObjectVersionRequest req) {
		if (Log.isLoggable(Level.INFO)) {
			Log.info("FetchObjectVersionRequest client = " + req.getClientId());
		}

		if (req.hasSubscription())
			getSession(req.getClientId()).subscribe(req.getUid());

		src.reply(doFetchVersionRequest(src, req));
	}

	private FetchObjectVersionReply doFetchVersionRequest(Envelope src, FetchObjectVersionRequest req) {
		if (Log.isLoggable(Level.INFO)) {
			Log.info("FetchObjectVersionRequest client = " + req.getClientId() + "; crdt id = " + req.getUid());
		}

		CausalityClock currentClockCopy = clocks.currentClockCopy();
		CMP_CLOCK cmp = req.getVersion() == null ? CMP_CLOCK.CMP_EQUALS : currentClockCopy.compareTo(req.getVersion());

		if (cmp.is(CMP_ISDOMINATED, CMP_CONCURRENT)) {
			Thread.dumpStack();
			// this.updateCurrentClock();
			currentClockCopy = clocks.currentClockCopy();
			cmp = currentClockCopy.compareTo(req.getVersion());
		}

		ManagedCRDT<?> crdt = getCRDT(req.getUid(), req.getVersion(), req.getClientId());

		if (crdt == null) {
			if (Log.isLoggable(Level.INFO)) {
				Log.info("END doFetchVersionRequest [not found]:" + req.getUid());
			}
			return new FetchObjectVersionReply(FetchStatus.OBJECT_NOT_FOUND);
		} else {

			crdt.augmentWithDCClockWithoutMappings(currentClockCopy);
			final FetchObjectVersionReply.FetchStatus status = (cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) ? FetchStatus.VERSION_NOT_FOUND : FetchStatus.OK;
			if (Log.isLoggable(Level.INFO)) {
				Log.info("END FetchObjectVersionRequest clock = " + crdt.getClock() + "/" + req.getUid());
			}

			return new FetchObjectVersionReply(status, crdt);
		}
	}

	@Override
	public void onReceive(final Envelope src, final CommitUpdatesRequest req) {
		if (Log.isLoggable(Level.INFO)) {
			Log.info("CommitUpdatesRequest client = " + req.getClientId() + ":ts=" + req.getCltTimestamp() + ":nops=" + req.getObjectUpdateGroups().size() + "/" + req.getTimestamp());
		}

		if (req.isReadOnly() || !clocks.record(req.getCltTimestamp(), clocks.clientClock)) {
			src.reply(new CommitUpdatesReply(clocks.clientClockCopy()));
		} else {

			req.setSource(src);
			fifo.queue4CommitTxn(req.getCltTimestamp().getIdentifier()).enqueue(req.getCltTimestamp().getCounter(), req, (CommitUpdatesRequest i) -> {
				if (Log.isLoggable(Level.INFO)) {
					Log.info("CommitUpdatesRequest ... SeqNo=" + i.getCltTimestamp());
				}
				if (clocks.cmp(clocks.currentClock, i.getDependencyClock()) == CMP_ISDOMINATED) {
					kStability.blockTransaction(i);
				} else {
					final ClientSession session = getSession(i.getClientId());
					if (i.getTimestamp() == null)
						prepareAndDoCommit(session, i);
					else {
						doOneCommit(session, i);
					}
				}
			});
		}
	}

	@Override
	public void onReceive(final Envelope src, final RemoteCommitUpdatesRequest req) {
		// System.out.println(siteId + " >>>>>>>>GOT K COMMIT FOR: " +
		// req.getTimestamp() + " deps: " + req.getDependencyClock());

		this.onReceive(Envelope.DISCARD, (CommitUpdatesRequest) req);
		src.reply(new RemoteCommitUpdatesReply());
	}

	protected void prepareAndDoCommit(final ClientSession session, final CommitUpdatesRequest req) {
		endpoint4servers.asyncRequest(sequencer, new GenerateTimestampRequest(req.getClientId(), req.getCltTimestamp(), req.getDependencyClock()), (GenerateTimestampReply reply) -> {
			req.setTimestamp(reply.getTimestamp());
			doOneCommit(session, req);
		});
	}

	protected void doOneCommit(final ClientSession session, final CommitUpdatesRequest req) {

		if (Log.isLoggable(Level.INFO)) {
			Log.info("CommitUpdatesRequest: doProcessOneCommit: cltTs = " + req.getCltTimestamp() + " :ts=" + req.getTimestamp() + " :nops=" + req.getObjectUpdateGroups().size());
		}

		final List<CRDTObjectUpdatesGroup<?>> ops = req.getObjectUpdateGroups();
		ops.forEach(op -> {
			op.addSystemTimestamp(req.getTimestamp());
		});

		CausalityClock currentClockCopy = clocks.currentClockCopy();
		currentClockCopy.record(req.getTimestamp());

		boolean execOK = execTxnCRDTOps(req, currentClockCopy);
		if (!execOK) // Execution failed...
			req.getSource().reply(new CommitUpdatesReply());
		else {
			endpoint4servers.asyncRequest(sequencer, new CommitTimestampRequest(req), (CommitTimestampReply i) -> {
				if (Log.isLoggable(Level.INFO)) {
					Log.info("Commit: received CommitTimestampReply vrs:" + i.getCurrentClock() + ";ts = " + req.getTimestamp());
				}

				// System.err.println(siteId +
				// "   Commit: received CommitTimestampReply vrs:" +
				// i.getCurrentClock() + ";ts = " + req.getTimestamp() +
				// " ;clt: " + req.getCltTimestamp());

					if (i.getStatus() == CommitTimestampReply.CommitTSStatus.OK) {
						if (Log.isLoggable(Level.INFO)) {
							Log.info("Commit OK: ts:" + req.getTimestamp());
						}

						if (req.kStability() >= 0)
							kStability.makeStable(req, () -> {
								req.getSource().reply(new CommitUpdatesReply(req.getTimestamp()));
							});

						i.getCurrentClock().record(req.getTimestamp());
						updateCurrentClock(i.getCurrentClock());
					}

				});
		}
	}
	public static void main(String[] args) {
		Args.use(args);

		List<String> dcsServers = Args.subList("-servers");
		System.err.println(dcsServers);
		new Server();
	}

	public class ClientSession extends RemoteSwiftSubscriber {

		ClientSession(String clientId) {
			super(clientId, suPubSub.stub());
		}

		public void subscribe(CRDTIdentifier key) {
			suPubSub.subscribe(key, this);
		}

		public void onNotification(UpdateNotification update) {
		}
	}

	public ClientSession getSession(String clientId) {
		ClientSession session = sessions.get(clientId), nsession;
		if (session == null) {
			session = sessions.putIfAbsent(clientId, nsession = new ClientSession(clientId));
			if (session == null)
				session = nsession;
		}
		return session;
	}

	Map<String, ClientSession> sessions = new ConcurrentHashMap<String, ClientSession>();
}
