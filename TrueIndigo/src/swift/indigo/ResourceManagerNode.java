package swift.indigo;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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

import swift.api.CRDTIdentifier;
import swift.application.test.TestsUtil;
import swift.clocks.Timestamp;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesReply.AcquireReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.ResourceCommittedRequest;
import swift.indigo.proto.TransferResourcesRequest;
import swift.utils.LogSiteFormatter;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Args;
import sys.utils.ConcurrentHashSet;
import sys.utils.Profiler;
import sys.utils.Threading;

public class ResourceManagerNode implements ReservationsProtocolHandler {
	protected static final long DEFAULT_QUEUE_PROCESSING_WAIT_TIME = 1;

	// private static final int DEFAULT_REQUEST_TRANSFER_RATIO = 3;

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

	private HashMap<CRDTIdentifier, ResourceRequest<?>> pendingRequests;

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
		this.pendingRequests = new HashMap<CRDTIdentifier, ResourceRequest<?>>();

		this.workers = Executors.newCachedThreadPool();
		this.manager = new IndigoResourceManager(sequencer, surrogate, endpoints, outgoingMessages, pendingRequests);
		this.stub = sequencer.stub;

		this.sequencer = sequencer;
		this.active = true;

		initLogging();

		final TransferFirstMessageBalacing messageBalancing = new TransferFirstMessageBalacing(incomingRequestsQueue, transferRequestsQueue);
		// Incoming requests processor thread

		new Thread(() -> {
			while (active) {
				IndigoOperation request = messageBalancing.nextOp();
				if (request != null)
					workers.execute(() -> {
						request.deliverTo(thisManager);
					});
				else
					Threading.sleep(DEFAULT_QUEUE_PROCESSING_WAIT_TIME);
			}
		}).start();

		new Thread(() -> {
			while (active) {
				TransferResourcesRequest request;
				synchronized (outgoingMessages) {
					while (outgoingMessages.isEmpty())
						Threading.waitOn(outgoingMessages, 10 * DEFAULT_QUEUE_PROCESSING_WAIT_TIME);
					request = outgoingMessages.poll();
				}
				if (request != null) {
					Endpoint endpoint = endpoints.get(request.getDestination());
					stub.send(endpoint, request);
				}
			}
		}).start();

	}
	public void process(TransferResourcesRequest request) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Processing TransferResourcesRequest: " + request);
		}
		TRANSFER_STATUS reply = manager.transferResources(request);
		if (reply.equals(TRANSFER_STATUS.SUCCESS) || reply.equals(TRANSFER_STATUS.ALREADY_SATISFIED)) {
			synchronized (pendingRequests) {
				for (ResourceRequest<?> req_i : request.getResources()) {
					pendingRequests.remove(req_i.getResourceId());
				}
			}
		}
		waitingIndex.remove(request);
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Finished TransferResourcesRequest: " + request + " Reply: " + reply);
		}
	}

	public void process(ResourceCommittedRequest request) {
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
			}
		}
		profiler.endOp(profilerName, opId);
		if (logger.isLoggable(Level.INFO))
			logger.info("Finished ReleaseResourcesRequest" + request);

	}
	public void processWithReply(Envelope conn, AcquireResourcesRequest request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Processing AcquireResourcesRequest " + request);
		AcquireResourcesReply reply = null;
		long opId = profiler.startOp(profilerName, "acquire");
		reply = manager.acquireResources(request);
		if (reply.acquiredStatus().equals(AcquireReply.YES)) {
			replies.put(request.getClientTs(), reply);
		}
		waitingIndex.remove(request);
		profiler.endOp(profilerName, opId);
		conn.reply(reply);
		if (logger.isLoggable(Level.INFO))
			logger.info("Finished AcquireResourcesRequest " + request + " Reply: " + reply + " " + reply.getSnapshot());

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
			if (checkAcquireAlreadyProcessed(request) != null) {
				if (logger.isLoggable(Level.INFO))
					logger.info("Received an already processed message: " + request + " REPLY: " + replies.get(request.getClientTs()));
				reply = replies.get(request.getClientTs());
			} else {
				if (isDuplicate(request)) {
					if (logger.isLoggable(Level.INFO))
						logger.info("ignore duplicate request: " + request);
				} else {
					synchronized (incomingRequestsQueue) {
						incomingRequestsQueue.add(request);
					}
				}
			}
		}
		if (reply != null)
			conn.reply(reply);
	}
	@Override
	public void onReceive(Envelope conn, TransferResourcesRequest request) {
		if (!isDuplicate(request)) {
			synchronized (transferRequestsQueue) {
				transferRequestsQueue.add(request);
			}
		} else {
			logger.info("repeated message");
		}
	}

	@Override
	public void onReceive(Envelope conn, ResourceCommittedRequest request) {
		AcquireResourcesReply reply = replies.get(request.getClientTs());

		synchronized (pendingRequests) {
			for (CRDTIdentifier id : request.getUpdatedCRDTs()) {
				Map<String, SortedSet<ResourceRequest<?>>> updatedResources = new HashMap<>();
				if (pendingRequests.containsKey(id)) {
					ResourceRequest<?> pendingRequest = pendingRequests.get(id);
					SortedSet<ResourceRequest<?>> sitePendingRequests = updatedResources.get(pendingRequest.getRequesterId());
					if (sitePendingRequests == null) {
						sitePendingRequests = new TreeSet<>();
						updatedResources.put(pendingRequest.getRequesterId(), sitePendingRequests);
					}
					sitePendingRequests.add(pendingRequest);
				}
				synchronized (incomingRequestsQueue) {
					for (Entry<String, SortedSet<ResourceRequest<?>>> entry : updatedResources.entrySet()) {
						TransferResourcesRequest transfer = new TransferResourcesRequest(entry.getKey(), sequencer.siteId, manager.transferSeqNumber.incrementAndGet(), entry.getValue());
						incomingRequestsQueue.add(transfer);
					}
				}
			}
		}

		if (reply != null && !reply.isReleased()) {
			if (!isDuplicate(request)) {
				boolean hasLock = false;
				for (ResourceRequest<?> resource : reply.getResourcesRequest()) {
					if (resource instanceof LockReservation) {
						hasLock = true;
						break;
					}
				}
				if (hasLock)
					incomingRequestsQueue.add(request);
				else {
					reply.setReleased();
				}
			}
		}
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
