package swift.dc;

import static swift.clocks.CausalityClock.CMP_CLOCK.CMP_ISDOMINATED;
import static sys.Context.Networking;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.logging.Level;
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

	final Map<String, Deque<CommitUpdatesRequest>> blockedTransactions;

	KStabilizer(Server server) {
		this.server = server;
		this.stub = server.endpoint4clts;

		this.sites = new HashMap<>();
		Args.subList("-servers").forEach(str -> {
			sites.putIfAbsent(new Url(str).siteId(), Networking.resolve(str, Defaults.SERVER_URL));
		});

		this.blockedTransactions = new ConcurrentHashMap<>();

		Log.info("Remote ENDPOINTS: " + sites);
	}

	void makeStable(CommitUpdatesRequest req, Runnable listener) {
		if (req.kStability() == 0)
			listener.run();

		final RemoteCommitUpdatesRequest rr = new RemoteCommitUpdatesRequest(req);
		Set<String> stable = new ConcurrentHashSet<>();
		sites.forEach((siteId, endpoint) -> {
			stub.asyncRequest(endpoint, rr, reply -> {
				stable.add(siteId);

				if (stable.size() == req.kStability()) {
					listener.run();
				}
			});
		});
	}
	void blockTransaction(CommitUpdatesRequest req) {
		req.blkTime = System.currentTimeMillis();
		getBlocked(req).addLast(req);
		if (Log.isLoggable(Level.INFO))
			Log.info(server.siteId + "   >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BLOCKING: " + req.getCltTimestamp() + "   " + req.getTimestamp() + "  deps:  " + req.getDependencyClock() + " now:" + server.clocks.currentClockCopy());
	}

	void checkBlockedTransactions() {
		CausalityClock clock = server.clocks.currentClockCopy();
		blockedTransactions.values().parallelStream().forEach(txns -> {

			List<CommitUpdatesRequest> done = new ArrayList<>();

			for (CommitUpdatesRequest req : txns)
				if (clock.compareTo(req.getDependencyClock()) != CMP_ISDOMINATED) {
					done.add(req);
					if (Log.isLoggable(Level.INFO))
						Log.info(server.siteId + " @@@@ Time spent blocking: " + (System.currentTimeMillis() - req.blkTime) + "   deps:" + req.getDependencyClock());
					server.doOneCommit(server.getSession(req.getClientId()), req);
				} else {
					if (Log.isLoggable(Level.INFO))
						Log.info(server.siteId + "now: " + clock + " @@@@ Still blocked  deps:" + req.getDependencyClock() + " after: " + (System.currentTimeMillis() - req.blkTime));
					break;
				}
			txns.removeAll(done);
		});

	}
	private Deque<CommitUpdatesRequest> getBlocked(CommitUpdatesRequest req) {
		String key = req.getCltTimestamp().getIdentifier();
		Deque<CommitUpdatesRequest> res = blockedTransactions.get(key), nres;
		if (res == null) {
			res = blockedTransactions.putIfAbsent(key, nres = new ConcurrentLinkedDeque<>());
			if (res == null)
				res = nres;
		}
		return res;
	}
}
