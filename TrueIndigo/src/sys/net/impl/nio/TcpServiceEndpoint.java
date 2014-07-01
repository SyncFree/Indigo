/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.net.impl.nio;

import static sys.Context.Networking;
import static sys.Context.Sys;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import sys.KryoLib;
import sys.net.api.Endpoint;
import sys.net.api.MessageHandler;
import sys.net.impl.GID;
import sys.net.impl.ServiceChannel;
import sys.net.impl.ServiceEndpoint;
import sys.net.impl.ServicePacket;
import sys.net.impl.TcpService;
import sys.net.impl.Url;
import sys.utils.IO;
import sys.utils.IP;
import sys.utils.Threading;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

final public class TcpServiceEndpoint extends ServiceEndpoint {

	private static Logger Log = Logger.getLogger(TcpServiceEndpoint.class.getName());

	final Url url;
	final GID gid;
	Endpoint localEndpoint;

	public TcpServiceEndpoint(Endpoint endpoint, MessageHandler msghandler, TcpService service) {
		super(msghandler, service);
		this.gid = new GID();
		this.url = new Url(endpoint.url());
		this.bind(false);
	}

	@Override
	public Endpoint localEndpoint() {
		return localEndpoint;
	}

	public void bind(boolean daemon) {
		try {
			ServerSocketChannel ssc = ServerSocketChannel.open();
			ssc.bind(new InetSocketAddress(url.getPort(-1)));
			Threading.newThread(daemon, () -> {
				for (;;) {
					try {
						new TcpChannel(ssc.accept());
					} catch (IOException x) {
						x.printStackTrace();
					}
				}
			}).start();;
			InetSocketAddress addr = (InetSocketAddress) ssc.getLocalAddress();
			localEndpoint = Networking.resolve(String.format("tcp://%s:%s", IP.localHostAddressString(), addr.getPort()));
		} catch (IOException x) {
			x.printStackTrace();
		}
	}

	public void connect(Endpoint remote) {
		try {
			Url url = new Url(remote.url());
			SocketChannel sc = SocketChannel.open();
			sc.connect(new InetSocketAddress(url.getHost(), url.getPort(-1)));
			InetSocketAddress addr = (InetSocketAddress) sc.getLocalAddress();
			localEndpoint = Networking.resolve(String.format("tcp://%s:%s", IP.localHostAddressString(), addr.getPort()));
			new TcpChannel(sc, remote).send(new GID());
		} catch (IOException x) {
			Log.warning("Cannot connect to:" + remote);
			x.printStackTrace();
			Threading.sleep(250);
		}
	}

	public class TcpChannel implements ServiceChannel {
		static final int ISIZE = Integer.SIZE / Byte.SIZE;

		Endpoint remoteEndpoint;
		final SocketChannel channel;
		final KryoOutputBuffer outBuf;

		TcpChannel(SocketChannel channel) {
			this.channel = channel;
			this.outBuf = new KryoOutputBuffer();
			this.setSocketOptions(channel.socket());
			Threading.newThread(true, () -> {
				try {
					KryoInputBuffer inBuf = new KryoInputBuffer();
					for (;;) {
						ServicePacket pkt = inBuf.readClassAndObject(channel);
						if (pkt != null) {
							Sys.downloadedBytes().addAndGet(inBuf.frameSize);
							pkt.dispatch(service, this, inBuf.frameSize);
						} else
							break;
					}
				} catch (Exception x) {
					x.printStackTrace();
					service.onFailedChannel(this, null);
				} finally {
					IO.close(channel);
					service.onClosedChannel(this);
				}
			}).start();
		}

		TcpChannel(SocketChannel channel, Endpoint remote) {
			this(channel);
			this.remoteEndpoint = remote;
			service.onNewChannel(this);
		}

		public synchronized void setRemoteGID(GID gid) {
			remoteEndpoint = Networking.resolve(String.format("tcp://%s:0/%s", IP.localHostAddressString(), gid));
			service.onNewChannel(this);
		}

		@Override
		synchronized public boolean send(Object obj) {
			try {
				int frameSize = outBuf.writeClassAndObject(obj, channel);
				Sys.uploadedBytes().addAndGet(frameSize);
			} catch (Exception x) {
				x.printStackTrace();
				service.onFailedChannel(this, x);
				IO.close(channel);
			}
			return true;
		}
		@Override
		public Endpoint localEndpoint() {
			return localEndpoint;
		}

		@Override
		public Endpoint remoteEndpoint() {
			return remoteEndpoint;
		}

		public String toString() {
			return remoteEndpoint.toString();
		}

		void setSocketOptions(Socket cs) {
			try {
				cs.setTcpNoDelay(true);
				cs.setReceiveBufferSize(1 << 20);
				cs.setSendBufferSize(1 << 20);
			} catch (Exception x) {
				x.printStackTrace();
			}
		}
	}

}

final class KryoInputBuffer {

	private static final int MAXUSES = 1024; // 2b replace in networking
												// constants...

	int uses = 0;
	Input in;
	ByteBuffer buffer;
	int frameSize;

	KryoInputBuffer() {
		buffer = ByteBuffer.allocate(8192);
		in = new Input(buffer.array());
	}

	final public int readFrom(SocketChannel ch) throws IOException {

		buffer.clear().limit(4);
		while (buffer.hasRemaining() && ch.read(buffer) > 0);

		if (buffer.hasRemaining())
			return -1;

		frameSize = buffer.getInt(0);

		ensureCapacity(frameSize);

		buffer.clear().limit(frameSize);
		while (buffer.hasRemaining() && ch.read(buffer) > 0);

		if (buffer.hasRemaining())
			return -1;

		buffer.flip();
		frameSize += 4;
		return frameSize;
	}

	@SuppressWarnings("unchecked")
	public <T> T readClassAndObject(SocketChannel ch) throws Exception {
		if (readFrom(ch) > 0) {
			in.setPosition(0);
			return (T) KryoLib.kryo().readClassAndObject(in);
		} else
			return null;
	}

	private void ensureCapacity(int required) {
		if (required > buffer.array().length || ++uses > MAXUSES) {
			buffer = ByteBuffer.allocate(nextPowerOfTwo(required));
			in.setBuffer(buffer.array());
			uses = 0;
		}
	}

	static private int nextPowerOfTwo(int value) {
		if (value == 0)
			return 1;
		if ((value & value - 1) == 0)
			return value;
		value |= value >> 1;
		value |= value >> 2;
		value |= value >> 4;
		value |= value >> 8;
		value |= value >> 16;
		return value + 1;
	}
}

final class KryoOutputBuffer {

	private static final int MAXUSES = 1024;

	int uses;
	Output out;
	ByteBuffer buffer;

	public KryoOutputBuffer() {
		uses = Integer.MAX_VALUE;
		reset();
	}

	private void reset() {
		if (uses++ > MAXUSES) {
			buffer = ByteBuffer.allocate(8192);
			out = new Output(buffer.array(), Integer.MAX_VALUE);
			uses = 0;
		}
	}

	public int writeClassAndObject(Object object, SocketChannel ch) throws Exception {
		reset();
		out.setPosition(4);

		KryoLib.kryo().writeClassAndObject(out, object);
		int length = out.position();

		if (length > buffer.capacity())
			buffer = ByteBuffer.wrap(out.getBuffer());

		buffer.clear();
		buffer.putInt(0, length - 4);
		buffer.limit(length);

		return ch.write(buffer);
	}
}