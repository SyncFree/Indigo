package swift.dc;

import static sys.Context.Networking;

import java.util.HashMap;
import java.util.Map;

import swift.proto.CommitUpdatesRequest;
import swift.proto.RemoteCommitUpdatesRequest;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.net.impl.Url;
import sys.utils.Args;
import sys.utils.ConcurrentHashSet;

import com.esotericsoftware.minlog.Log;

public class KStabilityService {

	final Server server;
	final Service stub;

	final Map<String, Endpoint> sites;

	KStabilityService(Server server) {
		this.server = server;
		this.stub = server.endpoint4clts;

		this.sites = new HashMap<>();
		Args.subList("-servers").forEach(str -> {
			sites.putIfAbsent(new Url(str).siteId(), Networking.resolve(str, Defaults.SERVER_URL));
		});

		System.err.println(sites);
		Log.info("Remote ENDPOINTS: " + sites);
	}

	void makeStable(CommitUpdatesRequest req, Runnable listener) {
		if (req.kStability() == 0)
			listener.run();

		final RemoteCommitUpdatesRequest rreq = new RemoteCommitUpdatesRequest(req);
		rreq.kUnstable = new ConcurrentHashSet<>(sites.keySet());
		sites.forEach((siteId, endpoint) -> {
			stub.asyncRequest(endpoint, rreq, reply -> {
				rreq.kUnstable.remove(siteId);
				if (sites.size() - rreq.kUnstable.size() == req.kStability())
					listener.run();
				System.err.println(reply);
			});
		});
	}
}
