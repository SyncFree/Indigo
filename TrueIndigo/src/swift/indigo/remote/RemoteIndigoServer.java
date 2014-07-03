package swift.indigo.remote;

import static sys.Context.Networking;

import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.indigo.Defaults;
import swift.indigo.IndigoServer;
import swift.indigo.proto.AcquireLocksReply;
import swift.indigo.proto.AcquireLocksRequest;
import swift.indigo.proto.CreateLocksRequest;
import swift.indigo.proto.DiscardSnapshotRequest;
import swift.indigo.proto.IndigoCommitRequest;
import swift.indigo.proto.IndigoProtocol;
import swift.indigo.proto.ReleaseLocksRequest;
import swift.indigo.proto.TransferReservationRequest;
import swift.proto.FetchObjectVersionRequest;
import swift.proto.SurrogateProtocol;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Args;

public class RemoteIndigoServer implements IndigoProtocol, SurrogateProtocol {
	private static Logger Log = Logger.getLogger(RemoteIndigoServer.class.getName());

	final Service stub;
	final IndigoServer server;
	final Endpoint lockManager;
	final boolean emulateWeakConsistency; // for evaluation...

	public RemoteIndigoServer(Endpoint lockManager, IndigoServer server) {
		this.server = server;
		this.lockManager = lockManager;

		this.emulateWeakConsistency = Args.contains("-weak");

		Endpoint localEndpoint = Networking.resolve(Args.valueOf("-indigo", Defaults.REMOTE_INDIGO_URL));
		this.stub = Networking.bind(localEndpoint, this);

		Log.info("Remote Indigo Server running @: " + this.stub.localEndpoint());
	}

	public void onReceive(final Envelope src, final AcquireLocksRequest req) {

		if (emulateWeakConsistency) {
			CausalityClock snapshot = server.clocks.currentClockCopy();
			src.reply(new AcquireLocksReply(server.registerSnapshot(snapshot), snapshot));
		} else {
			stub.asyncRequest(lockManager, req, (AcquireLocksReply r) -> {
				if (r != null) {
					src.reply(r.setSerial(server.registerSnapshot(r.getSnapshot())));
				}
			});
		}
	}
	@Override
	public void onReceive(final Envelope src, final DiscardSnapshotRequest request) {
		server.disposeSnapshot(request.serial());
	}

	public void onReceive(final Envelope conn, final ReleaseLocksRequest req) {
		if (!emulateWeakConsistency) {
			stub.send(lockManager, req);
			server.disposeSnapshot(req.serial());
		}
	}

	public void onReceive(final Envelope src, final IndigoCommitRequest req) {
		server.onReceive(src, req);
		server.disposeSnapshot(req.serial());
	}

	public void onReceive(final Envelope src, final FetchObjectVersionRequest req) {
		server.onReceive(src, req);
	}

	public void onReceive(Envelope src, TransferReservationRequest request) {
		server.onReceive(src, request);
	}

	public void onReceive(final Envelope src, final CreateLocksRequest req) {
		throw new RuntimeException("Unexpected Operation CreateLocksRequest:" + req);
	}
}
