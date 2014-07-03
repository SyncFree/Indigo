package sys.net.impl;

import java.util.concurrent.atomic.AtomicLong;

import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Handler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ServicePacket implements KryoSerializable, Envelope {

	Object payload;
	long handlerId;

	transient int msgSize;
	transient TcpService service;
	transient ServiceChannel channel;

	public ServicePacket() {
	}

	ServicePacket(Object payload, Handler<?> handler) {
		this(payload, handler == null ? 0L : g_replyHandlerId.incrementAndGet());
	}

	ServicePacket(Object payload, Handler<?> handler, boolean streamingReplies) {
		this(payload, handler == null ? 0L : g_replyHandlerId.incrementAndGet() + 1L);
	}

	ServicePacket(Object payload, long handlerId) {
		this.payload = payload;
		this.handlerId = handlerId;
	}

	public void dispatch(TcpService service, ServiceChannel channel, int msgSize) {
		this.service = service;
		this.channel = channel;
		this.msgSize = msgSize;
		service.dispatch(this);
	}

	@Override
	public void read(Kryo kryo, Input in) {
		this.handlerId = in.readLong();
		this.payload = kryo.readClassAndObject(in);
	}

	@Override
	public void write(Kryo kryo, Output out) {
		out.writeLong(this.handlerId);
		kryo.writeClassAndObject(out, this.payload);
	}

	@Override
	public Endpoint sender() {
		return channel.remoteEndpoint();
	}

	@Override
	public <T> void reply(T msg) {
		service.reply(this, msg);
	}

	@Override
	public int msgSize() {
		return msgSize;
	}

	public String toString() {
		return handlerId + "  " + payload.getClass();
	}

	static AtomicLong g_replyHandlerId = new AtomicLong(8L);
}