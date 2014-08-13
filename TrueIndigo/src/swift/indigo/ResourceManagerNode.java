package swift.indigo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.clocks.Timestamp;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesReply.AcquireReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.ReleaseResourcesRequest;
import swift.indigo.proto.RequestWithReply;
import swift.indigo.proto.TransferResourcesRequest;
import swift.proto.ClientRequest;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;

public class ResourceManagerNode implements ReservationsProtocolHandler {

	protected static final long DEFAULT_QUEUE_PROCESSING_WAIT_TIME = 50;

	private static final int DEFAULT_REQUEST_TRANSFER_RATIO = 3;

	private static Logger logger = Logger.getLogger(ResourceManagerNode.class.getName());

	private IndigoResourceManager manager;

	// Incoming requests
	private Queue<IndigoOperation> incomingRequestsQueue;

	private Map<Timestamp, ClientRequest> beingProcessed;

	// Outgoing requests
	private transient PriorityQueue<TransferResourcesRequest> transferRequestsQueue;

	private boolean active;

	private Service stub;

	private Map<Timestamp, AcquireResourcesReply> replies = new ConcurrentHashMap<Timestamp, AcquireResourcesReply>();

	private IndigoSequencerAndResourceManager sequencer;

	private ResourceManagerNode thisManager = this;

	private Set<TransferResourcesRequest> duplicateTransfers;

	private Queue<TransferResourcesRequest> incomingTransfers;

	public ResourceManagerNode(IndigoSequencerAndResourceManager sequencer, Endpoint surrogate,
			final Map<String, Endpoint> endpoints) {

		// Outgoing transfers queue
		Queue<TransferResourcesRequest> outgoingMessages = new LinkedList<>();

		// TODO: Add ordering function
		this.transferRequestsQueue = new PriorityQueue<TransferResourcesRequest>();
		this.incomingRequestsQueue = new LinkedList<IndigoOperation>();
		this.beingProcessed = new HashMap<Timestamp, ClientRequest>();
		this.incomingTransfers = new LinkedList<TransferResourcesRequest>();
		this.duplicateTransfers = new HashSet<TransferResourcesRequest>();
		this.manager = new IndigoResourceManager(sequencer, surrogate, endpoints, outgoingMessages);
		this.stub = sequencer.stub;

		this.sequencer = sequencer;
		this.active = true;

		final SimpleMessageBalacing messageBalancing = new SimpleMessageBalacing(DEFAULT_REQUEST_TRANSFER_RATIO,
				incomingRequestsQueue, transferRequestsQueue);

		// Incoming requests processor thread
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (active) {
					IndigoOperation request;
					synchronized (thisManager) {
						// System.out.println(sequencer.siteId + " QUEUE: "
						// + incomingRequests);
						request = messageBalancing.nextOp();
					}
					if (request != null) {
						request.deliverTo(thisManager);
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
					if (outgoingMessages.size() > 0) {
						TransferResourcesRequest request = null;
						synchronized (thisManager) {
							if (outgoingMessages.size() > 0) {
								request = outgoingMessages.remove();
							} else
								continue;
						}
						Endpoint endpoint = endpoints.get(request.getDestination());
						logger.info("Asking resources: " + request);
						stub.send(endpoint, request);
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

	}
	public void process(TransferResourcesRequest request) {
		synchronized (thisManager) {
			if (logger.isLoggable(Level.INFO))
				logger.info("SITE: " + sequencer.siteId + " Processing TransferResourcesRequest: " + request);
			TRANSFER_STATUS reply = manager.transferResources(request);
			if (reply.hasTransferred()) {
				duplicateTransfers.add(request);
			}
			if (logger.isLoggable(Level.INFO)) {
				logger.info("SITE: " + sequencer.siteId + " Finished TransferResourcesRequest: " + request + " Reply: "
						+ reply);
			}
		}
	}

	public void process(ReleaseResourcesRequest request) {
		synchronized (thisManager) {
			if (logger.isLoggable(Level.INFO))
				logger.info("SITE: " + sequencer.siteId + " Processing ReleaseResourcesRequest " + request);

			Timestamp ts = request.getClientTs();
			AcquireResourcesReply alr = replies.get(ts);
			if (alr != null && !alr.isReleased()) {
				// replies.remove(ts);
				alr.setReleased();
				if (alr.acquiredResources()) {
					manager.releaseResources(alr);
				} else {
					logger.warning("SITE: " + sequencer.siteId + " Trying to release but did not get resources "
							+ request);
				}
			}
			if (logger.isLoggable(Level.INFO))
				logger.info("SITE: " + sequencer.siteId + " Finished ReleaseResourcesRequest" + request);
		}
	}

	public void processWithReply(Envelope conn, AcquireResourcesRequest request) {
		AcquireResourcesReply reply = null;
		synchronized (thisManager) {
			beingProcessed.put(request.getClientTs(), request);
			if (logger.isLoggable(Level.INFO))
				logger.info("SITE: " + sequencer.siteId + " Processing AcquireResourcesRequest " + request);
			reply = checkAlreadyProcessed(request);
			// Handle repeated messages
			if (reply == null) {
				if (request.getRequests().size() > 0) {
					reply = manager.acquireResources(request);
					if (reply.acquiredStatus().equals(AcquireReply.YES)) {
						replies.put(request.getClientTs(), reply);
					}
				} else {
					reply = new AcquireResourcesReply(AcquireReply.NO_RESOURCES, sequencer.clocks.currentClockCopy());
				}
			} else {
				reply = new AcquireResourcesReply(AcquireReply.REPEATED, sequencer.clocks.currentClockCopy());
			}

			if (logger.isLoggable(Level.INFO))
				logger.info("SITE: " + sequencer.siteId + " Finished AcquireResourcesRequest " + request + " Reply: "
						+ reply);

			beingProcessed.remove(request.getClientTs());
		}
		conn.reply(reply);
	}
	/**
	 * Message handlers
	 */

	@Override
	public void onReceive(Envelope conn, AcquireResourcesRequest request) {
		RequestWithReply requestWR = new RequestWithReply(conn, request);
		AcquireResourcesReply reply = null;

		if (request.getRequests().size() == 0) {
			reply = new AcquireResourcesReply(AcquireReply.NO_RESOURCES, sequencer.clocks.currentClockCopy());
		} else {
			synchronized (thisManager) {
				if (!incomingRequestsQueue.contains(requestWR) && !beingProcessed.containsKey(request.getClientTs())
						&& !replies.containsKey(request.getClientTs())) {
					incomingRequestsQueue.add(requestWR);
				} else {
					if (logger.isLoggable(Level.INFO))
						logger.info(sequencer.siteId + " Received an already processed message: " + request);
					reply = new AcquireResourcesReply(AcquireReply.REPEATED, sequencer.clocks.currentClockCopy());
				}
			}
		}
		if (reply != null)
			conn.reply(reply);

	}
	@Override
	public void onReceive(Envelope conn, TransferResourcesRequest request) {
		incomingTransfers.add(request);
		synchronized (thisManager) {
			if (!duplicateTransfers.contains(request)) {
				transferRequestsQueue.add(request);
			}
		}
	}

	@Override
	public void onReceive(Envelope conn, ReleaseResourcesRequest request) {
		synchronized (thisManager) {
			incomingRequestsQueue.add(request);
		}
	}

	@Override
	public void onReceive(Envelope conn, AcquireResourcesReply request) {
		logger.warning("RPC " + request.getClass() + " not implemented!");
	}

	/**
	 * Private methods
	 */

	private AcquireResourcesReply checkAlreadyProcessed(AcquireResourcesRequest request) {
		AcquireResourcesReply reply = replies.get(request.getClientTs());

		if (reply != null && logger.isLoggable(Level.INFO))
			logger.info("SITE: " + sequencer.siteId + " Reply from cache: " + reply);

		if (reply != null)
			return reply;
		return null;
	}

}

class SimpleMessageBalacing {

	enum OPType {
		TRANSFER, REQUEST
	};

	private AtomicInteger transfers;
	private AtomicInteger requests;
	private final int ratio;

	private Queue<IndigoOperation> requestQueue;
	private Queue<TransferResourcesRequest> transferQueue;

	public SimpleMessageBalacing(int requestTransferRatio, Queue<IndigoOperation> requestQueue,
			Queue<TransferResourcesRequest> transferQueue) {
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

	public synchronized IndigoOperation nextOp() {
		int nRequests = requests.get();
		int nTransfers = transfers.get();
		if (requestQueue.size() > 0 && (nRequests - nTransfers <= ratio || transferQueue.size() == 0)) {
			registerOp(OPType.REQUEST);
			return requestQueue.remove();
		} else if (transferQueue.size() > 0) {
			registerOp(OPType.TRANSFER);
			return transferQueue.remove();
		} else
			return null;

	}
}
