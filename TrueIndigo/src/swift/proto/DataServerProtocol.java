package swift.proto;

import static sys.utils.NotImplemented.NotImplemented;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public interface DataServerProtocol extends MessageHandler {

	default void onReceive(final Envelope src, final DHTGetCRDT req) {
		throw NotImplemented;
	}

	default void onReceive(final Envelope src, final DHTExecCRDT req) {
		throw NotImplemented;
	}

}
