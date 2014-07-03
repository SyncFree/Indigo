package swift.dc;

import static swift.clocks.CausalityClock.CMP_CLOCK.CMP_ISDOMINATED;
import static sys.Context.Networking;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import swift.clocks.CausalityClock;
import swift.proto.CommitUpdatesRequest;
import swift.proto.RemoteCommitUpdatesRequest;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.net.impl.Url;
import sys.utils.Args;
import sys.utils.ConcurrentHashSet;

public class KStabilizer {
	static Logger Log = Logger.getLogger(Server.class.getName());

	final Server server;
	final Service stub;

	final Map<String, Endpoint> sites;

	final Map<CommitUpdatesRequest, Long> blockedTransactions;

	KStabilizer(Server server) {
		this.server = server;
		this.stub = server.endpoint4clts;

		this.sites = new HashMap<>();
		Args.subList("-servers").forEach(str -> {
			sites.putIfAbsent(new Url(str).siteId(), Networking.resolve(str, Defaults.SERVER_URL));
		});

		this.blockedTransactions = new ConcurrentHashMap<>();

		System.err.println(sites);
		Log.info("Remote ENDPOINTS: " + sites);
	}

	void makeStable(CommitUpdatesRequest req, Runnable listener) {
		if (req.kStability() == 0)
			listener.run();

		final RemoteCommitUpdatesRequest rr = new RemoteCommitUpdatesRequest(req);
		rr.pendingStability = new ConcurrentHashSet<>(sites.keySet());
		sites.forEach((siteId, endpoint) -> {
			stub.asyncRequest(endpoint, rr, reply -> {
				rr.pendingStability.remove(siteId);

				if (sites.size() - rr.pendingStability.size() == req.kStability())
					listener.run();

			});
		});
	}

	void blockTransaction(CommitUpdatesRequest req) {
		blockedTransactions.put(req, 0L);
	}
	void checkBlockedTransactions() {
		CausalityClock clock = server.clocks.currentClockCopy();
		blockedTransactions.forEach((req, v) -> {
			if (clock.compareTo(req.getDependencyClock()) != CMP_ISDOMINATED) {
				blockedTransactions.remove(req);
				server.doOneCommit(server.getSession(req.getClientId()), req);
			}
		});
	}
}
