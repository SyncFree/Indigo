/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.indigo;

import static sys.Context.Networking;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.dc.Defaults;
import swift.dc.Sequencer;
import swift.indigo.proto.AcquireResourcesReply;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.indigo.proto.InitializeResources;
import swift.indigo.proto.ReleaseResourcesRequest;
import swift.indigo.proto.TransferResourcesRequest;
import swift.proto.CommitTimestampRequest;
import swift.proto.GenerateTimestampReply;
import swift.proto.GenerateTimestampRequest;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.impl.Url;
import sys.utils.Args;

/**
 * 
 * @author smduarte
 * 
 */
public class IndigoSequencerAndResourceManager extends Sequencer implements ReservationsProtocolHandler {
	private static Logger logger = Logger.getLogger(IndigoSequencerAndResourceManager.class.getName());

	ResourceManagerNode lockManagerNode;
	List<String> dcNames;

	IndigoSequencerAndResourceManager(String siteId, int port, List<String> servers, List<String> sequencers, String sequencerShadow, boolean isBackup, Properties props, List<String> dcNames) {
		// super(siteId, port, servers, sequencers, sequencerShadow, isBackup,
		// props);
		this.dcNames = dcNames;
	}

	@Override
	public void start() {
		super.start();
		Map<String, Endpoint> endpoints = new HashMap<String, Endpoint>();
		Args.subList("-sequencers").forEach(str -> {
			endpoints.put(new Url(str).siteId(), Networking.resolve(str, Defaults.SEQUENCER_URL));
		});

		Endpoint surrogate = Networking.resolve(Args.valueOf("-server", ""), Defaults.SERVER_URL4SEQUENCERS);
		lockManagerNode = new ResourceManagerNode(this, surrogate, endpoints);
	}

	@Override
	public void onReceive(final Envelope conn, final GenerateTimestampRequest request) {
		conn.reply(new GenerateTimestampReply(super.clocks.newTimestamp(), request.getCltTimestamp()));
	}

	@Override
	public void onReceive(final Envelope conn, final CommitTimestampRequest request) {
		// RPC Handle is null, since this should not reply EVER to the client
		if (logger.isLoggable(Level.INFO))
			logger.info("Commit timestamp " + request.getTimestamp() + " " + request.getCommitUpdatesRequest());
		super.onReceive(conn, request);

		// TODO:
		// try to release any resources properly...
		lockManagerNode.onReceive(null, new ReleaseResourcesRequest(request.getCltTimestamp()));
	}

	@Override
	public void onReceive(Envelope conn, AcquireResourcesRequest request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got AcquireResourcesRequest for request: " + request);
		lockManagerNode.onReceive(conn, request);
	}

	@Override
	public void onReceive(Envelope conn, AcquireResourcesReply request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got AcquireResourcesReply:" + request);
		lockManagerNode.onReceive(null, request);
	}

	@Override
	public void onReceive(Envelope conn, ReleaseResourcesRequest request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got ReleaseResourcesRequest:" + request);
		lockManagerNode.onReceive(conn, request);
	}

	public void onReceive(Envelope conn, final TransferResourcesRequest request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got TransferResourcesRequest:" + request);
		lockManagerNode.onReceive(conn, request);
	}

	public void onReceive(Envelope conn, final InitializeResources request) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Got InitializeResources:" + request);
		lockManagerNode.onReceive(conn, request);
	}

	public static void main(String[] args) {
		Args.use(args);

		int port = Args.valueOf("-port", -1);
		String siteId = Args.valueOf("-name", "X");
		boolean isBackup = Args.valueOf("-backup", false);

		List<String> servers = Args.subList("-servers");
		List<String> sequencers = Args.subList("-sequencers");
		String sequencerShadow = Args.valueOf("-sequencerShadow", (String) null);

		List<String> dcNames = Args.subList("-names");

		Properties props = new Properties();

		new IndigoSequencerAndResourceManager(siteId, port, servers, sequencers, sequencerShadow, isBackup, props, dcNames).start();
	}

}
