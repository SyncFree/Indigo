package swift.indigo;

import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import swift.application.test.TestsUtil;
import swift.clocks.Timestamp;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesReply.AcquireReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.ReleaseResourcesRequest;
import swift.indigo.proto.TransferResourcesRequest;
import swift.utils.LogSiteFormatter;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Args;
import sys.utils.ConcurrentHashSet;
import sys.utils.Profiler;

public class ResourceManagerNode implements ReservationsProtocolHandler {

	protected static final long DEFAULT_QUEUE_PROCESSING_WAIT_TIME = 1;

	private static final int DEFAULT_REQUEST_TRANSFER_RATIO = 3;

	private static final int nWorkers = 10;

	private IndigoResourceManager manager;

	// Incoming requests
	private Queue<IndigoOperation> incomingRequestsQueue;

	private transient PriorityQueue<TransferResourcesRequest> transferRequestsQueue;

	private boolean active;

	private Service stub;

	private Map<Timestamp, AcquireResourcesReply> replies = new ConcurrentHashMap<Timestamp, AcquireResourcesReply>();

	private IndigoSequencerAndResourceManager sequencer;

	private ResourceManagerNode thisManager = this;

	private Set<IndigoOperation> waitingIndex;

	private Map<Timestamp, IndigoOperation> alreadyProcessedTransfers;

	private ExecutorService workers;

	private static Profiler profiler;

	private static String profilerName = "ManagerProfile";

	private Logger logger;

	public ResourceManagerNode(IndigoSequencerAndResourceManager sequencer, Endpoint surrogate, final Map<String, Endpoint> endpoints) {

		// Outgoing transfers queue
		Queue<TransferResourcesRequest> outgoingMessages = new LinkedList<>();

		// Incoming transfer requests are ordered by priority: promotes
		// exclusive_lock operations
		// and messages with smaller requests (size of requests was not tested)
		this.transferRequestsQueue = new PriorityQueue<TransferResourcesRequest>();
		// Incoming messages are ordered by FIFO order
		this.incomingRequestsQueue = new ConcurrentLinkedQueue<IndigoOperation>();

		this.waitingIndex = new ConcurrentHashSet<IndigoOperation>();
		this.alreadyProcessedTransfers = new ConcurrentHashMap<Timestamp, IndigoOperation>();

		this.workers = Executors.newFixedThreadPool(nWorkers);
		this.manager = new IndigoResourceManager(sequencer, surrogate, endpoints, outgoingMessages);
		this.stub = sequencer.stub;

		this.sequencer = sequencer;
		this.active = true;

		initLogging();

		final TransferFirstMessageBalacing messageBalancing = new TransferFirstMessageBalacing(incomingRequestsQueue, transferRequestsQueue);

		ConcurrentHashMap<String, Long> sentTransfters = new ConcurrentHashMap<>();

		// Incoming requests processor thread
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (active) {
					IndigoOperation request;
					request = messageBalancing.nextOp();
					if (request != null) {
						workers.execute(new Runnable() {
							@Override
							public void run() {
								request.deliverTo(thisManager);
							}
						});
					} else {
						try {
							Thread.sleep(DEFAULT_QUEUE_PROCESSING_WAIT_TIME);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}).start();

		// Transfer requests thread
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (active) {
					TransferResourcesRequest request = null;
					Endpoint endpoint = null;
					synchronized (outgoingMessages) {
						if (outgoingMessages.size() > 0) {
							request = outgoingMessages.remove();
							endpoint = endpoints.get(request.getDestination());
						}
						if (request != null) {
							String key = request.key();
							long now = System.currentTimeMillis();
							Long ts = sentTransfters.get(key);
							if (ts == null || (now - ts) > 100) {
								sentTransfters.put(key, now);
								stub.send(endpoint, request);
							}
						} else {
							try {
								Thread.sleep(DEFAULT_QUEUE_PROCESSING_WAIT_TIME);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}
			}

		}).start();

	}
	public void process(TransferResourcesRequest request) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Processing TransferResourcesRequest: " + request);
		}

		TRANSFER_STATUS reply = manager.transferResources(request);

		// Never keep reply
		// if (reply.hasTransferred()) {
		// alreadyProcessedTransfers.put(request.getClientTs(), request);
		waitingIndex.remove(request);
		// }

		if (logger.isLoggable(Level.INFO)) {
			logger.info("Finished TransferResourcesRequest: " + request + " Reply: " + reply);
		}
	}
	public void process(ReleaseResourcesRequest request) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Processing ReleaseResourcesRequest " + request);
		}
		long opId = profiler.startOp(profilerName, "release");
		Timestamp ts = request.getClientTs();
		AcquireResourcesReply arr = replies.get(ts);
		if (arr != null && !arr.isReleased()) {
			if (arr.acquiredResources()) {
				manager.releaseResources(arr);
				arr.setReleased();
				waitingIndex.remove(request);
			} else {
				if (logger.isLoggable(Level.WARNING))
					logger.warning("Trying to release but did not get resources: exiting, should not happen " + request);
				System.exit(0);
			}
			if (logger.isLoggable(Level.INFO))
				logger.info("Finished ReleaseResourcesRequest" + request);
		}
		profiler.endOp(profilerName, opId);
	}

	public void processWithReply(Envelope conn, AcquireResourcesRequest request) {
		long opId = profiler.startOp(profilerName, "acquire");
		AcquireResourcesReply reply = null;
		if (logger.isLoggable(Level.INFO))
			logger.info("Processing AcquireResourcesRequest " + request);
		reply = manager.acquireResources(request);
		if (reply.acquiredStatus().equals(AcquireReply.YES)) {
			replies.put(request.getClientTs(), reply);
		}
		if (logger.isLoggable(Level.INFO))
			logger.info("Finished AcquireResourcesRequest " + request + " Reply: " + reply + " " + reply.getSnapshot());

		waitingIndex.remove(request);
		profiler.endOp(profilerName, opId);
		conn.reply(reply);
	}
	/**
	 * Message handlers
	 */

	@Override
	public void onReceive(Envelope conn, AcquireResourcesRequest request) {
		request.setHandler(conn);
		AcquireResourcesReply reply = null;
		profiler.trackRequest(profilerName, request);
		if (request.getResources().size() == 0) {
			reply = new AcquireResourcesReply(AcquireReply.NO_RESOURCES, sequencer.clocks.currentClockCopy());
		} else {
			if (isDuplicate(request)) {
				if (logger.isLoggable(Level.INFO))
					logger.info("ignore duplicate request: " + request);
				// reply = replies.get(request.getClientTs());
			} else if (checkAcquireAlreadyProcessed(request) != null) {
				if (logger.isLoggable(Level.INFO))
					logger.info("Received an already processed message: " + request + " REPLY: "
							+ replies.get(request.getClientTs()));
				reply = replies.get(request.getClientTs());
			} else {
				synchronized (incomingRequestsQueue) {
					incomingRequestsQueue.add(request);
				}
			}

		}
		if (reply != null)
			conn.reply(reply);
	}

	@Override
	public void onReceive(Envelope conn, TransferResourcesRequest request) {
		// if (!alreadyProcessedTransfers.containsKey(request.getClientTs())) {
		// Check if the message is duplicated
		if (!isDuplicate(request)) {
			synchronized (transferRequestsQueue) {
				transferRequestsQueue.add(request);
			}
		} else {
			logger.info("repeated message");
		}
		// } else {
		// logger.info("already processed request " + request);
		// }
	}

	@Override
	public void onReceive(Envelope conn, ReleaseResourcesRequest request) {
		AcquireResourcesReply reply = replies.get(request);
		if (reply == null || !reply.isReleased()) {
			if (!isDuplicate(request)) {
				incomingRequestsQueue.add(request);
			}
		}
	}

	@Override
	public void onReceive(Envelope conn, AcquireResourcesReply request) {
		if (logger.isLoggable(Level.WARNING))
			logger.warning("RPC " + request.getClass() + " not implemented!");
	}

	/**
	 * Private methods
	 */

	// If messages is already enqueued for processing ignore new request
	private boolean isDuplicate(IndigoOperation request) {
		if (!waitingIndex.add(request))
			return true;
		else
			return false;
	}

	private AcquireResourcesReply checkAcquireAlreadyProcessed(AcquireResourcesRequest request) {
		AcquireResourcesReply reply = replies.get(request.getClientTs());

		if (reply != null && logger.isLoggable(Level.INFO))
			logger.info("Reply from cache: " + reply);

		if (reply != null)
			return reply;
		return null;
	}

	private void initLogging() {

		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new LogSiteFormatter(sequencer.siteId));
		logger = Logger.getLogger(this.getClass().getName() + "." + sequencer.siteId);
		logger.setUseParentHandlers(false);
		logger.addHandler(handler);

		Logger logger = Logger.getLogger(profilerName);
		profiler = Profiler.getInstance();
		if (logger.isLoggable(Level.FINEST)) {
			FileHandler fileTxt;
			try {
				String resultsDir = Args.valueOf("-results_dir", ".");
				String siteId = Args.valueOf("-siteId", "GLOBAL");
				String suffix = Args.valueOf("-fileNameSuffix", "");
				fileTxt = new FileHandler(resultsDir + "/manager_profiler" + "_" + siteId + suffix + ".log");
				fileTxt.setFormatter(new java.util.logging.Formatter() {
					@Override
					public String format(LogRecord record) {
						return record.getMessage() + "\n";
					}
				});
				logger.addHandler(fileTxt);
				profiler.printMessage(profilerName, TestsUtil.dumpArgs());
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		profiler.printHeaderWithCustomFields(profilerName);

	}

}

class SimpleMessageBalacing {

	enum OPType {
		TRANSFER, REQUEST
	}

	private AtomicInteger transfers;
	private AtomicInteger requests;
	private final int ratio;

	private Queue<IndigoOperation> requestQueue;
	private Queue<TransferResourcesRequest> transferQueue;

	public SimpleMessageBalacing(int requestTransferRatio, Queue<IndigoOperation> requestQueue, Queue<TransferResourcesRequest> transferQueue) {
		this.ratio = requestTransferRatio;
		this.requestQueue = requestQueue;
		this.transferQueue = transferQueue;
		this.requests = new AtomicInteger();
		this.transfers = new AtomicInteger();
	}

	private void registerOp(OPType op) {
		int count;
		switch (op) {
			case TRANSFER :
				count = transfers.incrementAndGet();
				transfers.set(0);
				break;
			case REQUEST :
				count = requests.incrementAndGet();
				if (count == ratio)
					requests.set(0);
				break;
		}
	}

	public IndigoOperation nextOp() {
		int nRequests, nTransfers;
		synchronized (this) {
			nRequests = requests.get();
			nTransfers = transfers.get();
		}
		if (requestQueue.size() > 0 && (nRequests - nTransfers <= ratio || transferQueue.size() == 0)) {
			registerOp(OPType.REQUEST);
			return requestQueue.remove();
		} else if (transferQueue.size() > 0) {
			registerOp(OPType.TRANSFER);
			synchronized (transferQueue) {
				return transferQueue.remove();
			}
		} else
			return null;
	}

	public String toString() {
		return "REQ: " + requestQueue + " TRANS: " + transferQueue;
	}
}

class TransferFirstMessageBalacing {

	enum OPType {
		TRANSFER, REQUEST
	}

	private Queue<IndigoOperation> requestQueue;
	private Queue<TransferResourcesRequest> transferQueue;

	public TransferFirstMessageBalacing(Queue<IndigoOperation> requestQueue, Queue<TransferResourcesRequest> transferQueue) {
		this.requestQueue = requestQueue;
		this.transferQueue = transferQueue;
	}

	public IndigoOperation nextOp() {
		IndigoOperation op = null;
		if (transferQueue.size() > 0) {
			synchronized (transferQueue) {
				op = transferQueue.remove();
			}
		} else if (requestQueue.size() > 0) {
			synchronized (requestQueue) {
				op = requestQueue.remove();
			}
		}
		return op;
	}

	public String toString() {
		return "REQ: " + requestQueue + " TRANS: " + transferQueue;
	}

}
