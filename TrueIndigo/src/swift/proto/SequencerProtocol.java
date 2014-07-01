package swift.proto;

import static sys.utils.NotImplemented.NotImplemented;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public interface SequencerProtocol extends MessageHandler {

	default void onReceive(final Envelope e, final CurrentClockRequest r) {
		throw NotImplemented;
	}

	default void onReceive(final Envelope e, final CommitTimestampRequest r) {
		throw NotImplemented;
	}

	default boolean onReceive(final Envelope e, final GenerateTimestampRequest r) {
		throw NotImplemented;
	}
}
