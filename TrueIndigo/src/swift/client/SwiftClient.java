package swift.client;

import static sys.Context.Networking;
import swift.api.IsolationLevel;
import swift.api.SwiftScout;
import swift.api.SwiftSession;
import swift.api.TxnHandle;
import swift.crdt.core.CachePolicy;
import swift.exceptions.SwiftException;
import swift.indigo.Defaults;
import swift.indigo.remote.RemoteIndigo;
import sys.net.api.Endpoint;

public class SwiftClient implements SwiftSession {

	String sessionId;
	String serverUrl;
	RemoteIndigo indigo;

	SwiftClient(String sessionId, String serverUrl) {
		this.sessionId = sessionId;

		if (serverUrl.equals("localhost") || serverUrl.equals("*"))
			this.serverUrl = Defaults.REMOTE_INDIGO_URL;
		else
			this.serverUrl = serverUrl;

		Endpoint server = Networking.resolve(this.serverUrl, Defaults.REMOTE_INDIGO_URL);
		this.indigo = (RemoteIndigo) RemoteIndigo.getInstance(server);
	}
	static public SwiftSession newSession(String sessionId, String serverUrl) {
		return new SwiftClient(sessionId, serverUrl);
	}

	@Override
	public TxnHandle beginTxn(IsolationLevel isolationLevel, CachePolicy cachePolicy, boolean readOnly)
			throws SwiftException {
		indigo.beginTxn();
		return indigo.getTxnHandle();
	}

	@Override
	public void stopScout(boolean waitForCommit) {
	}

	@Override
	public String getSessionId() {
		return sessionId;
	}

	@Override
	public SwiftScout getScout() {
		throw new RuntimeException("Not implemented...");
	}

	@Override
	public void printStatistics() {
		throw new RuntimeException("Not implemented...");
	}

}
