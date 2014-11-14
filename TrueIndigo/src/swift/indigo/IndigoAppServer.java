/**
-------------------------------------------------------------------

Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

-------------------------------------------------------------------
**/
package swift.indigo;

import java.util.Properties;

import swift.indigo.remote.RemoteIndigoServer;

/**
 * Single server replying to client request. This class is used only to allow
 * client testing.
 * 
 * @author smduarte
 * 
 */
public class IndigoAppServer {

	static protected IndigoAppServer singleton;
	protected IndigoServer server;
	String sequencerHost;
	String lockManagerHost;

	Properties props;
	boolean emulateWeakConsistency;

	protected RemoteIndigoServer rIndigoServer;

	public IndigoAppServer(String sequencerHost, String lockManager, Properties props, boolean emulateWeakConsistency) {
		this.props = props;
		this.sequencerHost = sequencerHost;
		this.lockManagerHost = lockManager;
		this.emulateWeakConsistency = emulateWeakConsistency;
	}

	public static Indigo getIndigo() {
		if (singleton != null)
			return singleton.server.getIndigoInstance();
		else
			throw new RuntimeException("Indigo Server Not Running/Initalized...");
	}

	void startServer() {

		// Endpoint sequencer = Networking.resolve(sequencerUrl,
		// SEQUENCER_PORT);
		// Endpoint lockManager = Networking.resolve(lockManagerHost,
		// SEQUENCER_PORT);
		//
		// server = new IndigoServer();
		// rIndigoServer = new RemoteIndigoServer(sequencer, server);
	}

	public static void main(String[] args) {

		// String sequencerNode = Args.valueOf(args, "-sequencer", "localhost");
		// int portSurrogate = Args.valueOf(args, "-portSurrogate",
		// SURROGATE_PORT);
		//
		// boolean emulateWeakConsistency = Args.contains(args, "-weak");
		// String lockManagerNode = Args.valueOf(args, "-lockmanager",
		// sequencerNode);
		// singleton = new IndigoAppServer(sequencerNode, lockManagerNode,
		// props, emulateWeakConsistency);
		// singleton.startServer(portSurrogate, SURROGATE_PORT_FOR_SEQUENCERS);
	}

}
