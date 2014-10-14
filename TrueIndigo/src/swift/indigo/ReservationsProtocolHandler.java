package swift.indigo;

import static sys.utils.NotImplemented.NotImplemented;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.DiscardSnapshotRequest;
import swift.indigo.proto.IndigoCommitRequest;
import swift.indigo.proto.InitializeResources;
import swift.indigo.proto.ResourceCommittedRequest;
import swift.indigo.proto.TransferResourcesRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public interface ReservationsProtocolHandler extends MessageHandler {

	default void onReceive(Envelope conn, AcquireResourcesRequest request) {
		throw NotImplemented;
	}

	default void onReceive(Envelope conn, ResourceCommittedRequest request) {
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
