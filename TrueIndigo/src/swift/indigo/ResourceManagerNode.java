package swift.indigo;

import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
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

    private static Logger logger = Logger.getLogger(ResourceManagerNode.class.getName());

    private IndigoResourceManager manager;

    // Incoming requests
    private Queue<ClientRequest> incomingRequests;

    // Outgoing requests
    private transient PriorityQueue<TransferResourcesRequest> transferRequestsQueue;

    private boolean active;

    private Service stub;

    private Map<Timestamp, AcquireResourcesReply> replies = new ConcurrentHashMap<Timestamp, AcquireResourcesReply>();

    private IndigoSequencerAndResourceManager sequencer;

    public ResourceManagerNode(IndigoSequencerAndResourceManager sequencer, Endpoint surrogate,
            final Map<String, Endpoint> endpoints) {

        // TODO: Add ordering function
        transferRequestsQueue = new PriorityQueue<TransferResourcesRequest>();
        this.incomingRequests = new LinkedList<ClientRequest>();

        this.manager = new IndigoResourceManager(sequencer, surrogate, endpoints, transferRequestsQueue);
        this.stub = sequencer.stub;

        this.sequencer = sequencer;
        this.active = true;

        ResourceManagerNode monitor = this;

        new Thread(new Runnable() {

            @Override
            public void run() {
                while (active) {
                    if (incomingRequests.size() > 0) {
                        ClientRequest request = incomingRequests.remove();
                        if (request instanceof RequestWithReply) {
                            RequestWithReply rwr = ((RequestWithReply) request);
                            processWithReply(rwr.getHandle(), (AcquireResourcesRequest) rwr.getRequest());
                        } else {
                            if (request instanceof TransferResourcesRequest) {
                                process((TransferResourcesRequest) request);
                            } else {
                                process((ReleaseResourcesRequest) request);
                            }
                        }
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

        new Thread(new Runnable() {

            @Override
            public void run() {
                while (active) {
                    if (transferRequestsQueue.size() > 0) {
                        TransferResourcesRequest request = null;
                        synchronized (monitor) {
                            if (transferRequestsQueue.size() > 0) {
                                request = transferRequestsQueue.remove();
                            } else
                                continue;
                        }
                        Endpoint endpoint = endpoints.get(request.getDestination());
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

        // srvEndpoint.getFactory().toService(RpcServices.PUBSUB.ordinal(), new
        // SwiftProtocolHandler() {
        // public void onReceive(RpcHandle conn, PubSubNotification
        // notification) {
        // }
        // });
    }

    public synchronized void process(TransferResourcesRequest request) {
        if (logger.isLoggable(Level.INFO))
            logger.info("Processing TransferResourcesRequest" + request);
        TRANSFER_STATUS reply = manager.transferResources(request);
        if (logger.isLoggable(Level.INFO)) {
            logger.info("Finished TransferResourcesRequest: " + request + " " + reply);
        }
    }

    public synchronized void process(ReleaseResourcesRequest request) {
        if (logger.isLoggable(Level.INFO))
            logger.info("Processing ReleaseResourcesRequest" + request);

        Timestamp ts = request.getClientTs();
        AcquireResourcesReply alr = replies.get(ts);
        if (alr != null) {
            replies.remove(ts);
            manager.releaseResources(alr);
        }
        if (logger.isLoggable(Level.INFO))
            logger.info("Finished ReleaseResourcesRequest" + request);
    }

    public void processWithReply(Envelope conn, AcquireResourcesRequest request) {
        AcquireResourcesReply reply;
        synchronized (this) {
            if (logger.isLoggable(Level.INFO))
                logger.info("Processing AcquireResourcesRequest " + request);
            reply = checkAlreadyProcessed(request);
            reply = null;
            // Handle repeated messages
            if (reply == null) {
                if (request.getRequests().size() > 0) {
                    reply = manager.acquireResources(request);
                    if (reply.acquiredStatus().equals(AcquireReply.YES))
                        replies.put(request.getClientTs(), reply);
                } else {
                    reply = new AcquireResourcesReply(AcquireReply.YES, sequencer.clocks.currentClockCopy());
                }
            }
            if (logger.isLoggable(Level.INFO))
                logger.info("Finished AcquireResourcesRequest reply " + reply);

        }
        System.out.println("Manager reply to client " + reply);
        conn.reply(reply);
    }

    /**
     * Message handlers
     */

    @Override
    public void onReceive(Envelope conn, AcquireResourcesRequest request) {
        RequestWithReply requestWR = new RequestWithReply(conn, request);
        if (!incomingRequests.contains(requestWR)) {
            incomingRequests.add(requestWR);
        } else {
            if (logger.isLoggable(Level.INFO))
                logger.info("Duplicate request");
            conn.reply(new AcquireResourcesReply(AcquireReply.NO, sequencer.clocks.currentClockCopy()));
        }

    }

    @Override
    public void onReceive(Envelope conn, TransferResourcesRequest request) {
        incomingRequests.add(request);
    }

    @Override
    public void onReceive(Envelope conn, ReleaseResourcesRequest request) {
        // process(request);
        incomingRequests.add(request);
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

        if (logger.isLoggable(Level.INFO))
            logger.info("REPLY CACHE: " + reply);

        if (reply != null && reply.matches(request.getClientTs()))
            return reply;
        return null;
    }

}
