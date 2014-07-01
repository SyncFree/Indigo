package sys.net.impl;

import sys.Context;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class GID extends ServicePacket {
	long value;

	public GID() {
		this.value = Context.Sys.random().nextLong() >>> 1;
	}

	public String toString() {
		return Long.toString(value, 32);
	}

	public void dispatch(TcpService handler, ServiceChannel channel, int msgSize) {
		channel.setRemoteGID(this);
	}

	@Override
	public void read(Kryo kryo, Input in) {
		this.value = in.readLong();
	}

	@Override
	public void write(Kryo kryo, Output out) {
		out.writeLong(this.value);
	}
}
