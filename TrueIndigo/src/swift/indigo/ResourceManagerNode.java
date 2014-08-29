package swift.indigo;

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
import swift.indigo.proto.TransferResourcesRequest;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.ConcurrentHashSet;

public class ResourceManagerNode implements ReservationsProtocolHandler {

	protected static final long DEFAULT_QUEUE_PROCESSING_WAIT_TIME = 50;

	private static final int DEFAULT_REQUEST_TRANSFER_RATIO = 3;

	private static Logger logger = Logger.getLogger(ResourceManagerNode.class.getName());

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

	public ResourceManagerNode(IndigoSequencerAndResourceManager sequencer, Endpoint surrogate,
			final Map<String, Endpoint> endpoints) {

		// Outgoing transfers queue
		Queue<TransferResourcesRequest> outgoingMessages = new LinkedList<>();

		// Incoming transfer requests are ordered by priority: promotes
		// exclusive_lock operations
		// and messages with smaller requests (size of requests was not tested)
		this.transferRequestsQueue = new PriorityQueue<TransferResourcesRequest>();
		// Incoming messages are ordered by FIFO order
		this.incomingRequestsQueue = new LinkedList<IndigoOperation>();

		this.waitingIndex = new ConcurrentHashSet<IndigoOperation>();
		this.alreadyProcessedTransfers = new ConcurrentHashMap<Timestamp, IndigoOperation>();

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
		if (logger.isLoggable(Level.INFO)) {
			logger.info("SITE: " + sequencer.siteId + " Processing TransferResourcesRequest: " + request);
		}

		// TODO: Attention! this is not a synchronized call --- It blocked
		// during the transaction commit
		TRANSFER_STATUS reply = manager.transferResources(request);

		if (reply.hasTransferred()) {
			alreadyProcessedTransfers.put(request.getClientTs(), request);
			waitingIndex.remove(request);
		}

		if (logger.isLoggable(Level.INFO)) {
			logger.info("SITE: " + sequencer.siteId + " Finished TransferResourcesRequest: " + request + " Reply: "
					+ reply);
		}
	}
	public void process(ReleaseResourcesRequest request) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("SITE: " + sequencer.siteId + " Processing ReleaseResourcesRequest " + request);
		}
		Timestamp ts = request.getClientTs();
		AcquireResourcesReply arr = replies.get(ts);
		if (arr != null && !arr.isReleased()) {
			// replies.remove(ts);
			synchronized (thisManager) {
				if (arr.acquiredResources()) {
					// if (request.isRetry()) {
					// System.out.println("HERE");
					// }
					if (!manager.releaseResources(arr)) {
						// Failed - put it back on the queue
						incomingRequestsQueue.add(request);
						request.setRetry(true);
					} else {
						arr.setReleased();
						waitingIndex.remove(request);
					}
				} else {
					logger.warning("SITE: " + sequencer.siteId
							+ " Trying to release but did not get resources: exiting, should not happen " + request);
					System.exit(0);
				}
			}

			if (logger.isLoggable(Level.INFO))
				logger.info("SITE: " + sequencer.siteId + " Finished ReleaseResourcesRequest" + request);
		}
	}

	public void processWithReply(Envelope conn, AcquireResourcesRequest request) {
		AcquireResourcesReply reply = null;
		if (logger.isLoggable(Level.INFO))
			logger.info("SITE: " + sequencer.siteId + " Processing AcquireResourcesRequest " + request);
		synchronized (thisManager) {
			reply = manager.acquireResources(request);
		}
		if (reply.acquiredStatus().equals(AcquireReply.YES)) {
			replies.put(request.getClientTs(), reply);
		}
		if (logger.isLoggable(Level.INFO))
			logger.info("SITE: " + sequencer.siteId + " Finished AcquireResourcesRequest " + request + " Reply: "
					+ reply);

		waitingIndex.remove(request.getClientTs());
		conn.reply(reply);
	}
	/**
	 * Message handlers
	 */

	@Override
	public void onReceive(Envelope conn, AcquireResourcesRequest request) {
		request.setHandler(conn);
		AcquireResourcesReply reply = null;

		if (request.getRequests().size() == 0) {
			reply = new AcquireResourcesReply(AcquireReply.NO_RESOURCES, sequencer.clocks.currentClockCopy());
		} else {
			if (isDuplicate(request)) {
				if (logger.isLoggable(Level.INFO))
					logger.info(sequencer.siteId + " Message is already enqueued: " + request);
				reply = new AcquireResourcesReply(AcquireReply.REPEATED, sequencer.clocks.currentClockCopy());
			} else if (checkAcquireAlreadyProcessed(request) != null) {
				if (logger.isLoggable(Level.INFO))
					logger.info(sequencer.siteId + " Received an already processed message: " + request + " REPLY: "
							+ replies.get(request.getClientTs()));
				reply = new AcquireResourcesReply(AcquireReply.REPEATED, sequencer.clocks.currentClockCopy());
			} else {
				synchronized (thisManager) {
					incomingRequestsQueue.add(request);
				}
			}

		}
		if (reply != null)
			conn.reply(reply);
	}

	@Override
	public void onReceive(Envelope conn, TransferResourcesRequest request) {
		// Check if the transference request for the client timestamp
		// was already satisfied
		if (!alreadyProcessedTransfers.containsKey(request.getClientTs())) {
			// Check if the message is duplicated
			if (!isDuplicate(request)) {
				synchronized (thisManager) {
					transferRequestsQueue.add(request);
				}
			}
		}
	}

	@Override
	public void onReceive(Envelope conn, ReleaseResourcesRequest request) {
		AcquireResourcesReply reply = replies.get(request);
		if (reply == null || !reply.isReleased()) {
			if (!isDuplicate(request)) {
				synchronized (thisManager) {
					incomingRequestsQueue.add(request);
				}
			}
		}
	}

	@Override
	public void onReceive(Envelope conn, AcquireResourcesReply request) {
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
			logger.info("SITE: " + sequencer.siteId + " Reply from cache: " + reply);

		if (reply != null)
			return reply;
		return null;
	}

}

class FIFOClassQueue<T> {

	Queue<Queue<T>> orderedByPriority;

	public FIFOClassQueue(Queue<Queue<T>> orderedByPriority) {
		this.orderedByPriority = orderedByPriority;
	}

	public synchronized T nextElement() {
		for (Queue<T> queue : orderedByPriority) {
			if (!queue.isEmpty()) {
				return queue.remove();
			}
		}
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
