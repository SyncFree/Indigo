package test;

import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public interface TestProtocol extends MessageHandler {
	default void onReceive(Envelope e, Probe p) {
		Thread.dumpStack();
	}
}
