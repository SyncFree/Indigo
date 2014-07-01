package sys.net.impl.netty;

import static sys.Context.Networking;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import sys.KryoLib;
import sys.net.api.Endpoint;
import sys.net.api.MessageHandler;
import sys.net.impl.GID;
import sys.net.impl.ServiceChannel;
import sys.net.impl.ServiceEndpoint;
import sys.net.impl.ServicePacket;
import sys.net.impl.TcpService;
import sys.net.impl.Url;
import sys.utils.IP;
import sys.utils.Threading;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class TcpServiceEndpoint extends ServiceEndpoint {

	final Url url;
	final GID gid;

	Endpoint localEndpoint;
	EventLoopGroup bossGroup;
	EventLoopGroup workerGroup;

	public TcpServiceEndpoint(Endpoint endpoint, MessageHandler msghandler, TcpService service) {
		super(msghandler, service);
		this.gid = new GID();
		this.url = new Url(endpoint.url());
		this.bind();
		Thread.dumpStack();
	}

	@Override
	public Endpoint localEndpoint() {
		return localEndpoint;
	}

	void bind() {
		try {
			bossGroup = new NioEventLoopGroup();
			workerGroup = new NioEventLoopGroup();

			ServerBootstrap b = new ServerBootstrap();
			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					ch.config().setAutoRead(true);
					TcpChannel channel = new TcpChannel(ch);
					ch.pipeline().addLast(channel, new KryoMessageEncoder());
				}
			}).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true).childOption(ChannelOption.TCP_NODELAY, true);

			ChannelFuture f = b.bind(url.getPort(-1)).sync();
			InetSocketAddress addr = (InetSocketAddress) f.channel().localAddress();
			localEndpoint = Networking.resolve(String.format("tcp://%s:%s", IP.localHostAddressString(), addr.getPort()));
		} catch (Throwable t) {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			t.printStackTrace();
		}
	}
	public void connect(Endpoint remote) {
		try {
			workerGroup = new NioEventLoopGroup();

			Bootstrap b = new Bootstrap();
			b.group(workerGroup);
			b.channel(NioSocketChannel.class);
			b.option(ChannelOption.SO_KEEPALIVE, true).option(ChannelOption.TCP_NODELAY, true);

			final AtomicReference<TcpChannel> channel = new AtomicReference<TcpChannel>();
			b.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) throws Exception {
					channel.set(new TcpChannel(ch, remote));
					ch.pipeline().addLast(channel.get(), new KryoMessageEncoder());
				}
			});

			Url url = new Url(remote.url());
			ChannelFuture f = b.connect(url.getHost(), url.getPort(-1)).sync();
			GID gid = new GID();
			InetSocketAddress addr = (InetSocketAddress) f.channel().localAddress();
			localEndpoint = Networking.resolve(String.format("tcp://%s:%s/%s", IP.localHostAddressString(), addr.getPort(), gid));
			channel.get().send(gid);
			service.onNewChannel(channel.get());
		} catch (Exception x) {
			x.printStackTrace();
			Threading.sleep(100);
		}
	}

	public class TcpChannel extends ByteToMessageDecoder implements ServiceChannel {
		static final int ISIZE = Integer.SIZE / Byte.SIZE;

		Endpoint remoteEndpoint;
		final SocketChannel channel;

		TcpChannel(SocketChannel channel) {
			this.channel = channel;
		}

		TcpChannel(SocketChannel channel, Endpoint remote) {
			this.channel = channel;
			this.remoteEndpoint = remote;
		}

		public void setRemoteGID(GID gid) {
			remoteEndpoint = Networking.resolve(String.format("tcp://%s:0/%s", IP.localHostAddressString(), gid));
			service.onNewChannel(this);
		}

		protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) { // (2)
			if (in.readableBytes() > ISIZE && !service.isCongested()) {
				int frameSize = (in.getInt(in.readerIndex()) & 0x07FFFFFF) + ISIZE;
				if (in.isReadable(frameSize)) {
					Input inBuf = new Input(new ByteBufInputStream(in, frameSize));
					inBuf.readInt();
					ServicePacket pkt = (ServicePacket) KryoLib.kryo().readClassAndObject(inBuf);
					if (pkt != null) {
						System.err.println("----------------------->" + pkt);
						pkt.dispatch(service, this, frameSize);
					}
				}
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			cause.printStackTrace();
			ctx.close();
			service.onFailedChannel(this, cause);
		}

		@Override
		public boolean send(Object m) {
			channel.writeAndFlush(m).syncUninterruptibly();
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
	}
}

class KryoMessageEncoder extends MessageToByteEncoder<Object> {
	@Override
	protected void encode(ChannelHandlerContext ctx, Object obj, ByteBuf outBuf) {
		outBuf.writeInt(-1);
		Output out = new Output(new ByteBufOutputStream(outBuf));
		KryoLib.kryo().writeClassAndObject(out, obj);
		out.close();
		outBuf.setInt(0, outBuf.writerIndex() - 4);
	}
}
