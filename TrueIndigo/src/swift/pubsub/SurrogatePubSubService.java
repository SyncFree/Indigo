package swift.pubsub;

import static sys.Context.Networking;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.dc.Defaults;
import swift.dc.Server;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.pubsub.impl.AbstractPubSub;
import sys.utils.Args;
public class SurrogatePubSubService extends AbstractPubSub<CRDTIdentifier> {
	static Logger logger = Logger.getLogger(SurrogatePubSubService.class.getName());

	final Executor executor;
	final Service stub;
	final SwiftSubscriber handler;
	final Server surrogate;

	final FifoQueues fifoQueues = new FifoQueues();
	final CausalityClock minDcVersion = ClockFactory.newClock();
	final Map<String, CausalityClock> versions = new ConcurrentHashMap<String, CausalityClock>();

	public SurrogatePubSubService(Executor executor, final Server surrogate) {
		super(surrogate.serverId);

		this.executor = executor;
		this.surrogate = surrogate;

		this.handler = new SwiftSubscriber() {

			@Override
			public void onReceive(Envelope src, SwiftNotification evt) {
				if (logger.isLoggable(Level.INFO)) {
					logger.info("SwiftNotification payload = " + evt.payload());
				}
				fifoQueues.queueFor(evt.src(), new SwiftSubscriber() {
					public void onReceive(Envelope nil, SwiftNotification nextEvt) {

						updateDcVersions(nextEvt.src, nextEvt.dcVersion);
						nextEvt.payload().notifyTo(SurrogatePubSubService.this);

						// System.err.println(">>>>>" +
						// nextEvt.payload().getClass());
					}
				}).offer(evt.seqN(), evt);
			}

		};

		Endpoint localEndpoint = Networking.resolve(Args.valueOf("-pubsub", Defaults.PUBSUB_URL));
		System.err.println(localEndpoint);
		this.stub = Networking.bind(localEndpoint, this.handler);
	}

	public Service stub() {
		return stub;
	}

	public synchronized CausalityClock minDcVersion() {
		return minDcVersion.clone();
	}

	synchronized public void updateDcVersions(String srcId, CausalityClock estimate) {
		if (!srcId.equals(surrogate.serverId))
			versions.put(srcId, estimate);

		CausalityClock tmp = surrogate.clocks.currentClockCopy();
		versions.values().forEach(i -> {
			tmp.intersect(i);
		});

		minDcVersion.merge(tmp);
	}
}
