package sys.net.api;

import java.util.concurrent.atomic.AtomicLong;

import umontreal.iro.lecuyer.stat.Tally;

public interface Endpoint {

	String url();

	String url(int newPort);

	public AtomicLong incomingBytesCounter();

	public AtomicLong outgoingBytesCounter();

	public Tally rtt();
}
