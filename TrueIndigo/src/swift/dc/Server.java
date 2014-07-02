package swift.dc;

import static swift.clocks.CausalityClock.CMP_CLOCK.CMP_CONCURRENT;
import static swift.clocks.CausalityClock.CMP_CLOCK.CMP_ISDOMINATED;
import static swift.dc.Defaults.SERVER_URL;
import static swift.dc.Defaults.SERVER_URL4SEQUENCERS;
import static sys.Context.Networking;
import static sys.Context.Sys;

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
import swift.clocks.Timestamp;
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
	static Logger logger = Logger.getLogger(Server.class.getName());

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

	final Map<CommitUpdatesRequest, Long> blockedTransactions;

	final KStabilityService kStability;

	protected Server() {

		this.siteId = Args.valueOf("-siteId", "X");
		this.serverId = "s" + System.nanoTime();

		this.clocks = new Clocks("server");

		this.endpoint4clts = Networking.bind(Networking.resolve(Args.valueOf("-url", SERVER_URL)), this);
		this.endpoint4servers = Networking.bind(Networking.resolve(Args.valueOf("-url4seq", SERVER_URL4SEQUENCERS)), this);

		this.sequencer = Networking.resolve(Args.valueOf("-sequencer", ""), Defaults.SEQUENCER_URL);

		this.dataServer = new DataServer(this);

		this.kStability = new KStabilityService(this);

		this.suPubSub = new SurrogatePubSubService(generalExecutor, this);

		this.blockedTransactions = new ConcurrentHashMap<>();

		ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(512);
		crdtExecutor = new ThreadPoolExecutor(4, 8, 3, TimeUnit.SECONDS, workQueue);
		crdtExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

		Tasks.every(0.991, () -> {
			this.updateCurrentClock();
			this.checkBlockedTransactions();
		});

		if (logger.isLoggable(Level.INFO)) {
			logger.info("Server ready...");
		}
	}
	void updateCurrentClock() {
		endpoint4servers.asyncRequest(sequencer, new CurrentClockRequest(serverId), (CurrentClockReply r) -> {
			if (logger.isLoggable(Level.INFO)) {
				logger.info("CurrentClockReply: " + r.getClock());
			}
			clocks.updateCurrentClock(r.getClock());
		});
	}

	void checkBlockedTransactions() {
		CausalityClock clock = clocks.currentClockCopy();
		blockedTransactions.forEach((req, v) -> {
			if (clock.compareTo(req.getDependencyClock()) != CMP_ISDOMINATED) {
				blockedTransactions.remove(req);
				onReceive(req.getSource(), req);
			}
		});
	}

	<V extends CRDT<V>> boolean execTxnCRDTOps(final CommitUpdatesRequest req, final CausalityClock clock) {

		final List<CRDTObjectUpdatesGroup<?>> ops = req.getObjectUpdateGroups();

		final AtomicBoolean txnOK = new AtomicBoolean(true);
		if (ops.size() > 2) { // do multiple execCRDTs in parallel
			final Semaphore s = new Semaphore(0);
			ops.forEach(i -> {
				crdtExecutor.execute(() -> {
					try {
						System.err.println("Executing---->" + req.getTimestamp() + "/" + i);
						DHTExecCRDT execReq = new DHTExecCRDT(i, req.getCltTimestamp(), req.getPrvCltTimestamp(), clock);
						ExecCRDTResult res = dataServer.execCRDT(execReq);
						txnOK.compareAndSet(true, res.isResult());
					} finally {
						s.release();
					}
				});
			});
			s.acquireUninterruptibly(ops.size());
			System.err.println("-------------->>>>>EXECUTED DONE!!!" + ops.size());
		} else {
			ops.forEach(i -> {
				System.err.println("Executing---->" + req.getTimestamp() + "/" + i);
				DHTExecCRDT execReq = new DHTExecCRDT(i, req.getCltTimestamp(), req.getPrvCltTimestamp(), clock);
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
		if (logger.isLoggable(Level.INFO)) {
			logger.info("FetchObjectVersionRequest client = " + req.getClientId());
		}
		if (req.hasSubscription())
			getSession(req.getClientId()).subscribe(req.getUid());

		src.reply(doFetchVersionRequest(src, req));
	}

	private FetchObjectVersionReply doFetchVersionRequest(Envelope src, FetchObjectVersionRequest req) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("FetchObjectVersionRequest client = " + req.getClientId() + "; crdt id = " + req.getUid());
		}

		CMP_CLOCK cmp;
		CausalityClock currentClockCopy = clocks.currentClockCopy();
		cmp = req.getVersion() == null ? CMP_CLOCK.CMP_EQUALS : currentClockCopy.compareTo(req.getVersion());

		if (cmp.is(CMP_ISDOMINATED, CMP_CONCURRENT)) {
			this.updateCurrentClock();
			currentClockCopy = clocks.currentClockCopy();
			cmp = currentClockCopy.compareTo(req.getVersion());
		}

		ManagedCRDT<?> crdt = getCRDT(req.getUid(), req.getVersion(), req.getClientId());

		if (crdt == null) {
			if (logger.isLoggable(Level.INFO)) {
				logger.info("END doFetchVersionRequest [not found]:" + req.getUid());
			}
			return new FetchObjectVersionReply(FetchStatus.OBJECT_NOT_FOUND);
		} else {

			crdt.augmentWithDCClockWithoutMappings(currentClockCopy);

			final FetchObjectVersionReply.FetchStatus status = (cmp == CMP_CLOCK.CMP_ISDOMINATED || cmp == CMP_CLOCK.CMP_CONCURRENT) ? FetchStatus.VERSION_NOT_FOUND : FetchStatus.OK;
			if (logger.isLoggable(Level.INFO)) {
				logger.info("END FetchObjectVersionRequest clock = " + crdt.getClock() + "/" + req.getUid());
			}
			return new FetchObjectVersionReply(status, crdt);
		}
	}

	@Override
	public void onReceive(final Envelope src, final CommitUpdatesRequest req) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("CommitUpdatesRequest client = " + req.getClientId() + ":ts=" + req.getCltTimestamp() + ":nops=" + req.getObjectUpdateGroups().size());
		}
		final ClientSession session = getSession(req.getClientId());
		if (logger.isLoggable(Level.INFO)) {
			logger.info("CommitUpdatesRequest ... lastSeqNo=" + session.cltTimestamp());
		}

		req.setSource(src);
		Timestamp cltTs = session.cltTimestamp();
		if (cltTs != null && cltTs.getCounter() >= req.getCltTimestamp().getCounter() || req.isReadOnly())
			src.reply(new CommitUpdatesReply(clocks.clientClockCopy()));
		else {

			if (clocks.cmp(clocks.currentClock, req.getDependencyClock()) == CMP_ISDOMINATED) {
				blockedTransactions.put(req, Sys.timeMillis());
			} else {
				if (req.getTimestamp() == null)
					prepareAndDoCommit(session, req);
				else {
					req.setTimestamps(req.getTimestamp(), session.cltTimestamp());
					doOneCommit(session, req);
				}
			}
		}
	}

	protected void prepareAndDoCommit(final ClientSession session, final CommitUpdatesRequest req) {
		endpoint4servers.asyncRequest(sequencer, new GenerateTimestampRequest(req.getClientId(), req.getCltTimestamp(), req.getDependencyClock()), (GenerateTimestampReply reply) -> {
			req.setTimestamps(reply.getTimestamp(), session.cltTimestamp());
			doOneCommit(session, req);
			session.setCltTimestamp(req.getCltTimestamp());
		});
	}

	protected void doOneCommit(final ClientSession session, final CommitUpdatesRequest req) {

		if (logger.isLoggable(Level.INFO)) {
			logger.info("CommitUpdatesRequest: doProcessOneCommit: client = " + req.getClientId() + ":ts=" + req.getCltTimestamp() + ":nops=" + req.getObjectUpdateGroups().size());
		}

		final List<CRDTObjectUpdatesGroup<?>> ops = req.getObjectUpdateGroups();
		ops.forEach(op -> {
			op.addSystemTimestamp(req.getTimestamp());
		});

		req.getDependencyClock().record(req.getTimestamp());

		CausalityClock currentClockCopy = clocks.currentClockCopy();
		currentClockCopy.record(req.getTimestamp());

		boolean execOK = execTxnCRDTOps(req, currentClockCopy);
		if (!execOK) // Execution failed...
			req.getSource().reply(new CommitUpdatesReply());
		else {
			session.setCltTimestamp(req.getCltTimestamp());
			endpoint4servers.asyncRequest(sequencer, new CommitTimestampRequest(req.getTimestamp(), req.getCltTimestamp()), (CommitTimestampReply r) -> {
				if (logger.isLoggable(Level.INFO)) {
					logger.info("Commit: received CommitTimestampReply vrs:" + r.getCurrentClock() + ";ts = " + req.getTimestamp());
				}

				r.getCurrentClock().record(req.getTimestamp());
				clocks.updateCurrentClock(r.getCurrentClock());

				if (r.getStatus() == CommitTimestampReply.CommitTSStatus.OK) {
					if (logger.isLoggable(Level.INFO)) {
						logger.info("Commit OK: ts:" + req.getTimestamp());
					}

					req.setKStability(0);
					kStability.makeStable(req, () -> {
						req.getSource().reply(new CommitUpdatesReply(req.getTimestamp()));
					});
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
		Timestamp cltTimestamp;

		ClientSession(String clientId) {
			super(clientId, suPubSub.stub());
		}

		public Timestamp cltTimestamp() {
			return cltTimestamp;
		}

		void setCltTimestamp(Timestamp ts) {
			cltTimestamp = ts;
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
