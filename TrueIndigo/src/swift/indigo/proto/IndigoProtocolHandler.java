package swift.indigo.proto;

import static sys.utils.NotImplemented.NotImplemented;
import swift.proto.SurrogateProtocol;
import sys.net.api.Envelope;

public interface IndigoProtocolHandler extends SurrogateProtocol {

	default void onReceive(Envelope conn, final FetchObjectRequest request) {
		throw NotImplemented;
	}

	default void onReceive(Envelope src, DiscardSnapshotRequest request) {
		throw NotImplemented;
	}
}
