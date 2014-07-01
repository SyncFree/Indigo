package test;

import static sys.Context.Networking;

import java.util.concurrent.atomic.AtomicLong;

import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Message;
import sys.net.api.MessageHandler;
import sys.net.api.Service;
import sys.utils.Threading;
import umontreal.iro.lecuyer.stat.Tally;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class Client implements Runnable {

	@Override
	public void run() {
		Endpoint server = Networking.resolve("tcp://*:1234");

		final Tally rtt = new Tally("rtt");
		Service stub = Networking.stub();
		for (;;) {
			stub.asyncRequest(server, new Probe(), (Probe r) -> {
				rtt.add(r.rtt());
				if (rtt.numberObs() % 9999 == 0) {
					System.err.println(rtt.report());
					System.err.println(1000.0 / rtt.average());
				}
			});
			Threading.sleep(1);
		}
	}
	public static void main(String[] args) {
		new Client().run();
	}
}

class Probe implements Message, KryoSerializable {
	static AtomicLong g_serial = new AtomicLong(0L);

	long serial = g_serial.incrementAndGet();

	long timestamp = System.nanoTime();

	Probe() {
	}

	public double rtt() {
		return (System.nanoTime() - timestamp) * 1e-6;
	}

	@Override
	public void deliverTo(Envelope sender, MessageHandler handler) {
		((TestProtocol) handler).onReceive(sender, this);
	}

	@Override
	public void read(Kryo kryo, Input in) {
		timestamp = in.readLong();
		serial = in.readLong();
	}

	@Override
	public void write(Kryo kryo, Output out) {
		out.writeLong(timestamp);
		out.writeLong(serial);
	}

	public String toString() {
		return Long.toString(serial);
	}

}