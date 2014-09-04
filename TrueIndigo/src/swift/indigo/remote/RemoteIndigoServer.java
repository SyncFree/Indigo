package swift.indigo.remote;

import static sys.Context.Networking;

import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.indigo.Defaults;
import swift.indigo.IndigoServer;
import swift.indigo.ReservationsProtocolHandler;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.DiscardSnapshotRequest;
import swift.indigo.proto.IndigoCommitRequest;
import swift.indigo.proto.IndigoProtocolHandler;
import swift.indigo.proto.ReleaseResourcesRequest;
import swift.indigo.proto.TransferResourcesRequest;
import swift.proto.CommitUpdatesRequest;
import swift.proto.FetchObjectVersionRequest;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Args;

public class RemoteIndigoServer implements ReservationsProtocolHandler, IndigoProtocolHandler {
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

	public void onReceive(final Envelope src, final AcquireResourcesRequest req) {

		if (emulateWeakConsistency) {
			CausalityClock snapshot = server.clocks.currentClockCopy();
			src.reply(new AcquireResourcesReply(server.registerSnapshot(snapshot), snapshot));
		} else {
			stub.asyncRequest(lockManager, req, (AcquireResourcesReply r) -> {
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

	public void onReceive(final Envelope conn, final ReleaseResourcesRequest req) {
		if (!emulateWeakConsistency) {
			stub.send(lockManager, req);
			server.disposeSnapshot(req.serial());
		}
	}

	public void onReceive(final Envelope src, final IndigoCommitRequest req) {
		server.onReceive(src, (CommitUpdatesRequest) req);
		server.disposeSnapshot(req.serial());
	}

	public void onReceive(final Envelope src, final FetchObjectVersionRequest req) {
		server.onReceive(src, req);
	}

	public void onReceive(Envelope src, TransferResourcesRequest request) {
		server.onReceive(src, request);
	}

	@Override
	public void onReceive(Envelope conn, AcquireResourcesReply request) {
		System.out.println("Not implemented!!!");
		System.exit(0);

	}

}
