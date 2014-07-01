package swift.indigo.proto;

import static sys.utils.NotImplemented.NotImplemented;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public interface IndigoProtocol extends MessageHandler {

	default public void onReceive(Envelope src, final AcquireLocksRequest request) {
		throw NotImplemented;
	}

	default public void onReceive(Envelope src, final ReleaseLocksRequest request) {
		throw NotImplemented;
	}

	default public void onReceive(Envelope src, final CreateLocksRequest request) {
		throw NotImplemented;
	}

	default public void onReceive(Envelope src, final TransferReservationRequest request) {
		throw NotImplemented;
	}

	default public void onReceive(Envelope src, final DiscardSnapshotRequest request) {
		throw NotImplemented;
	}

	default public void onReceive(Envelope src, final IndigoCommitRequest request) {
		throw NotImplemented;
	}

}
