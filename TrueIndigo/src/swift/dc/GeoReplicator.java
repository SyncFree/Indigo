package swift.dc;

import static sys.Context.Networking;

import java.util.HashMap;
import java.util.Map;

import swift.proto.CommitUpdatesRequest;
import sys.net.api.Endpoint;
import sys.net.impl.Url;
import sys.utils.Args;

import com.esotericsoftware.minlog.Log;

public class GeoReplicator {

	final Server server;

	final Map<String, Endpoint> remotes;

	GeoReplicator(Server server) {
		this.server = server;

		this.remotes = new HashMap<String, Endpoint>();

		Args.subList("-sequencers").forEach(it -> {
			Url u = new Url(it);
			remotes.put(u.siteId(), Networking.resolve(it, Defaults.SEQUENCER_URL));
		});

		System.err.println(remotes);
		Log.info("Remote ENDPOINTS: " + remotes);
	}

	void geoReplicate(int kStability, CommitUpdatesRequest txn, Runnable listener) {

	}
}
