package swift.proto;

import static sys.utils.NotImplemented.NotImplemented;
import swift.indigo.proto.DiscardSnapshotRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public interface SurrogateProtocol extends MessageHandler {

    default void onReceive(Envelope src, FetchObjectVersionRequest r) {
        throw NotImplemented;
    }

    default void onReceive(Envelope src, CommitUpdatesRequest r) {
        throw NotImplemented;
    }

    default void onReceive(Envelope src, UnsubscribeUpdatesRequest r) {
        throw NotImplemented;
    }

    default void onReceive(Envelope src, RemoteCommitUpdatesRequest req) {
        throw NotImplemented;
    }

    default void onReceive(Envelope src, DiscardSnapshotRequest request) {
        throw NotImplemented;
    }
}
