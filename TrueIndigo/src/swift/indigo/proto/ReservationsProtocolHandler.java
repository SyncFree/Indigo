package swift.indigo.proto;

import static sys.utils.NotImplemented.NotImplemented;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public interface ReservationsProtocolHandler extends MessageHandler {

	default void onReceive(Envelope conn, AcquireResourcesRequest request) {
		throw NotImplemented;
	}

	default void onReceive(Envelope conn, AcquireResourcesReply request) {
		throw NotImplemented;
	}

	default void onReceive(Envelope conn, ReleaseResourcesRequest request) {
		throw NotImplemented;
	}

	default void onReceive(Envelope conn, TransferResourcesRequest request) {
		throw NotImplemented;
	}

	default void onReceive(Envelope conn, final InitializeResources request) {
		throw NotImplemented;
	}

	default void onReceive(Envelope src, final DiscardSnapshotRequest request) {
		throw NotImplemented;
	}

	default void onReceive(Envelope conn, final IndigoCommitRequest request) {
		throw NotImplemented;
	}

}
