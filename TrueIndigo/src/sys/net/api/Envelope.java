package sys.net.api;

import static sys.utils.NotImplemented.NotImplemented;

public interface Envelope {

	default Endpoint sender() {
		throw NotImplemented;
	}

	default <T> void reply(T msg) {
	}

	default int msgSize() {
		throw NotImplemented;
	}

	final Envelope DISCARD = new Envelope() {
	};
}
