package swift.indigo.remote;

import static sys.Context.Networking;

import java.util.concurrent.Callable;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.indigo.Defaults;
import swift.indigo.IndigoServer;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.DiscardSnapshotRequest;
import swift.indigo.proto.IndigoCommitRequest;
import swift.indigo.proto.IndigoProtocolHandler;
import swift.indigo.proto.ReleaseResourcesRequest;
import swift.indigo.proto.ReservationsProtocolHandler;
import swift.indigo.proto.TransferResourcesRequest;
import swift.proto.CommitUpdatesRequest;
import swift.proto.FetchObjectVersionRequest;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Args;
import sys.utils.Tasks;

public class RemoteIndigoServer implements ReservationsProtocolHandler, IndigoProtocolHandler {
	private static Logger Log = Logger.getLogger(RemoteIndigoServer.class.getName());

	final Service stub;
	final IndigoServer server;
	final Endpoint lockManager;
	final boolean emulateWeakConsistency; // for evaluation...
	final boolean emulateRedBlueConsistency;
	final boolean emulateStrongConsistency;

	final CausalityClock monotonicClock;

	public RemoteIndigoServer(Endpoint lockManager, IndigoServer server) {
		this.server = server;

		this.monotonicClock = ClockFactory.newClock();

		this.emulateWeakConsistency = Args.contains("-weak");
		this.emulateRedBlueConsistency = Args.contains("-redblue");
		this.emulateStrongConsistency = Args.contains("-strong");

		if (emulateWeakConsistency && emulateRedBlueConsistency) {
			System.err.println("Requested both weak & redblue consistency!!!");
			System.exit(0);
		}

		if (emulateRedBlueConsistency) {
			this.lockManager = Networking.resolve(Args.valueOf("-redblue", "???"), Defaults.SEQUENCER_URL);
		} else
			this.lockManager = lockManager;

		Endpoint localEndpoint = Networking.resolve(Args.valueOf("-indigo", Defaults.REMOTE_INDIGO_URL));
		this.stub = Networking.bind(localEndpoint, this);

		Log.info("Remote Indigo Server running @: " + this.stub.localEndpoint());
		if (emulateRedBlueConsistency || emulateWeakConsistency)
			Log.info(emulateRedBlueConsistency ? "Emulating redblue consistency: " + this.lockManager : "Fallback to weak consistency...");

	}

	private void monotonizeSnapshot(CausalityClock clock) {
		synchronized (monotonicClock) {
			clock.merge(monotonicClock);
			clock.merge(server.clocks.currentClockCopy());
			monotonicClock.merge(clock);
		}
	}

	public void onReceive(final Envelope src, final AcquireResourcesRequest req) {

		stub.asyncRequest(lockManager, req, (AcquireResourcesReply r) -> {
			if (r != null) {
				monotonizeSnapshot(r.getSnapshot());
				src.reply(r.setSerial(server.registerSnapshot(r.getSnapshot())));
			}
		});

		// if (emulateWeakConsistency || (!emulateStrongConsistency &&
		// req.getResources().isEmpty())) {
		// CausalityClock snapshot = server.clocks.currentClockCopy();
		// monotonizeSnapshot(snapshot);
		// src.reply(new
		// AcquireResourcesReply(server.registerSnapshot(snapshot), snapshot));
		// } else {
		// stub.asyncRequest(lockManager, req, (AcquireResourcesReply r) -> {
		// if (r != null) {
		// monotonizeSnapshot(r.getSnapshot());
		// src.reply(r.setSerial(server.registerSnapshot(r.getSnapshot())));
		// }
		// });
		// }
	}

	@Override
	public void onReceive(final Envelope src, final DiscardSnapshotRequest request) {
		server.disposeSnapshot(request.serial());
	}

	public void onReceive(final Envelope conn, final ReleaseResourcesRequest req) {
		if (!emulateWeakConsistency) {
			stub.send(lockManager, req);
		}
		server.disposeSnapshot(req.serial());
	}

	public void onReceive(final Envelope src, final IndigoCommitRequest req) {
		server.onReceive(src, (CommitUpdatesRequest) req);
		server.disposeSnapshot(req.serial());
	}

	public void onReceive(final Envelope src, final FetchObjectVersionRequest req) {

		Callable<Boolean> fetcher = () -> {
			CausalityClock.CMP_CLOCK cmp = req.getVersion().compareTo(server.clocks.currentClockCopy());
			if (cmp.is(CMP_CLOCK.CMP_EQUALS, CMP_CLOCK.CMP_ISDOMINATED)) {
				server.onReceive(src, req);
				return false;
			} else {
				Log.info("DC state is older than requested version:" + req.getVersion() + " will try again later...");
				return true;
			}
		};

		try {
			if (fetcher.call() == true)
				Tasks.every(0.01, fetcher);
		} catch (Exception e) {
			e.printStackTrace();
		}
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
