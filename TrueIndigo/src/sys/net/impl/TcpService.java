package sys.net.impl;

import static sys.Context.Networking;
import static sys.net.api.Networking.ServiceType.HIGH_CONTENTION;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import sys.net.api.Endpoint;
import sys.net.api.Handler;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.Networking.ServiceType;
import sys.net.api.Service;
import sys.utils.Threading;

public class TcpService implements Service, MessageHandler {

	private static Logger Log = Logger.getLogger(TcpService.class.getName());

	public static final int QUEUE_BACKPRESSURE_SIZE = 512;
	public static final int RETRIES = 3;
	public static final int RETRY_DELAY = 500;
	public static final int DEFAULT_TIMEOUT = 5000;

	int defaultTimeout;
	final ChannelManager mgr;
	final MessageHandler msgHandler;
	final ServiceEndpoint srvEndpoint;
	final ConcurrentHashMap<Long, Handler<?>> handlers = new ConcurrentHashMap<Long, Handler<?>>();

	final ServiceType type;
	final Executor executor;

	final AtomicInteger pendingTasks = new AtomicInteger(0);

	TcpService(Endpoint localEndpoint, MessageHandler msgHandler, ServiceType type) {
		this.msgHandler = msgHandler;
		this.mgr = new ChannelManager();
		this.type = ServiceType.LOW_LATENCY;
		this.defaultTimeout = DEFAULT_TIMEOUT;

		if (this.type == HIGH_CONTENTION) {
			this.srvEndpoint = new sys.net.impl.netty.TcpServiceEndpoint(localEndpoint, msgHandler, this);
			this.executor = new ThreadPoolExecutor(4, 4, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		} else {
			executor = null;
			this.srvEndpoint = new sys.net.impl.nio.TcpServiceEndpoint(localEndpoint, msgHandler, this);
		}
	}

	TcpService(ServiceType type) {
		this(Networking.resolve("tcp://*:0"), null, type);
	}

	public boolean isCongested() {
		return type == HIGH_CONTENTION && pendingTasks.get() >= QUEUE_BACKPRESSURE_SIZE;
	}

	public void dispatch(ServicePacket pkt) {
		if (type == ServiceType.LOW_LATENCY)
			this.onReceive(pkt);
		else {
			executor.execute(() -> {
				this.onReceive(pkt);
				pendingTasks.decrementAndGet();
			});
			pendingTasks.incrementAndGet();
		}
	}
	@Override
	public Endpoint localEndpoint() {
		return srvEndpoint.localEndpoint();
	}

	@Override
	public Service setDefaultTimeout(int ms) {
		this.defaultTimeout = ms;
		return this;
	}

	@Override
	public int getDefaultTimeout() {
		return defaultTimeout;
	}

	public <T> void reply(ServicePacket e, T m) {
		mgr.send(e.sender(), new ServicePacket(m, -e.handlerId));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void onReceive(ServicePacket pkt) {
		try {
			long hid = pkt.handlerId;
			if (hid >= 0L) {
				((Message) pkt.payload).deliverTo(pkt, msgHandler);
				pkt.payload = null;
			} else {
				hid = -hid;
				Handler handler = (hid & 1L) != 0L ? handlers.get(hid) : handlers.remove(hid);
				if (handler != null)
					handler.deliver(pkt.payload);
				else
					Log.warning("No handler for reply: " + pkt.payload);
			}
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	@Override
	public <T> T request(Endpoint dst, Message m) {
		return request(Integer.MAX_VALUE, dst, m);
	}

	@Override
	public <T> T request(int retries, Endpoint dst, Message m) {
		AtomicReference<T> replyRef = new AtomicReference<T>(null);
		do {
			synchronized (replyRef) {
				asyncRequest(dst, m, (T reply) -> {
					replyRef.set(reply);
					Threading.synchronizedNotifyAllOn(replyRef);
				});
				Threading.waitOn(replyRef, defaultTimeout);
			}
		} while (--retries >= 0 && replyRef.get() == null);
		return replyRef.get();
	}
	@Override
	public <T> void asyncRequest(Endpoint dst, Message m, Handler<T> replyHandler) {
		ServicePacket pkt = new ServicePacket(m, replyHandler);
		if (replyHandler != null)
			handlers.put(pkt.handlerId, replyHandler);

		mgr.send(dst, pkt);
	}

	@Override
	public <T> void asyncRequest(Endpoint dst, Message m, Handler<T> replyHandler, boolean streamingReplies) {
		ServicePacket pkt = new ServicePacket(m, replyHandler, streamingReplies);
		if (replyHandler != null)
			handlers.put(pkt.handlerId, replyHandler);
		mgr.send(dst, pkt);
	}

	@Override
	public void send(final Endpoint dst, final Message m) {
		mgr.send(dst, new ServicePacket(m, null));
	}

	public void onNewChannel(ServiceChannel ch) {
		mgr.add(ch);
	}

	public void onFailedChannel(ServiceChannel ch, Throwable causeOffailure) {
		mgr.remove(ch);
		// causeOffailure.printStackTrace();
	}

	public void onClosedChannel(ServiceChannel ch) {
		mgr.remove(ch);
	}

	final class ChannelManager {

		private final Logger Log = Logger.getLogger(ChannelManager.class.getName());

		boolean send(Endpoint remote, Object m) {
			for (int j = 0; j < RETRIES; j++) {
				int channels = 0;
				for (ServiceChannel i : channels(remote)) {
					channels++;
					if (i.send(m))
						return true;
				}
				if (channels == 0) {
					srvEndpoint.connect(remote);
					Threading.sleep((j + 1) * RETRY_DELAY);
				}
			}
			return false;
		}

		void add(ServiceChannel channel) {
			channels(channel.remoteEndpoint()).add(channel);
			Log.finest("Added connection to: " + channel.remoteEndpoint() + " : " + channels(channel.remoteEndpoint()));
		}

		void remove(ServiceChannel channel) {
			while (channels(channel.remoteEndpoint()).remove(channel));
			Log.finest("Removed connection to: " + channel.remoteEndpoint() + " : " + channels(channel.remoteEndpoint()));
		}

		CopyOnWriteArrayList<ServiceChannel> channels(Endpoint remote) {
			CopyOnWriteArrayList<ServiceChannel> cs = connections.get(remote), ncs;
			if (cs == null) {
				cs = connections.putIfAbsent(remote, ncs = new CopyOnWriteArrayList<ServiceChannel>());
				if (cs == null)
					cs = ncs;
			}
			return cs;
		}
	}

	final ConcurrentHashMap<Endpoint, CopyOnWriteArrayList<ServiceChannel>> connections = new ConcurrentHashMap<Endpoint, CopyOnWriteArrayList<ServiceChannel>>();
}
