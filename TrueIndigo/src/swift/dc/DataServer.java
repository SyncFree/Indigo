/*****************************************************************************
 * Copyright 2011-2014 INRIA
 * Copyright 2011-2014 Universidade Nova de Lisboa
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
package swift.dc;

import static sys.Context.Networking;
import static sys.utils.Threading.lock;
import static sys.utils.Threading.unlock;

import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTOperationDependencyPolicy;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.ObjectUpdatesInfo;
import swift.proto.DHTExecCRDT;
import swift.proto.DHTGetCRDT;
import swift.proto.DHTReply;
import swift.proto.DataServerProtocol;
import swift.pubsub.DataServerPubSubService;
import swift.pubsub.SurrogatePubSubService;
import swift.pubsub.UpdateNotification;
import swift.utils.LogSiteFormatter;
import sys.dht.DHT_Node;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Message;
import sys.net.api.Service;
import sys.utils.Args;
import sys.utils.Threading;
/**
 * Class to maintain data in the server.
 * 
 * @author preguica, smduarte
 */
public final class DataServer {
	final Service dht;
	final Clocks clocks;
	final Storage storage;

	final Server server;
	final SurrogatePubSubService suPubSub;
	final DataServerPubSubService dsPubSub;

	private static Logger logger;

	DataServer(Server server) {

		this.server = server;
		this.storage = new Storage();
		this.clocks = new Clocks("DataServer");

		this.suPubSub = server.suPubSub;
		this.dsPubSub = new DataServerPubSubService(server, server.generalExecutor);
		this.dht = initDHT();
		DHT_Node.init(server.siteId, Args.subList("-dht"), dht.localEndpoint());

		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new LogSiteFormatter(server.siteId));
		logger = Logger.getLogger(DataServer.class.getName() + "." + server.siteId);
		logger.setUseParentHandlers(false);
		logger.addHandler(handler);

		if (logger.isLoggable(Level.INFO)) {
			logger.info("Data server ready...");
		}

	}
	/**
	 * Start DHT subsystem...
	 */

	@SuppressWarnings("unchecked")
	<V> V dhtRequest(Endpoint dst, final Message req) {
		final AtomicReference<Object> result = new AtomicReference<Object>(req);
		for (; result.get() == req;) {
			synchronized (result) {
				dht.asyncRequest(dst, req, (DHTReply reply) -> {
					result.set(reply.payload());
					Threading.synchronizedNotifyAllOn(result);
				});
				Threading.waitOn(result, 100);
			}
		}
		return (V) result.get();
	}

	Service initDHT() {
		// first element of non empty list is local address always, otherwise
		// use default...
		String dhtUrl = Args.contains("-dht") ? Args.subList("-dht").get(0) : Defaults.DATASERVER_DHT_URL;

		Endpoint dhtEndpoint = Networking.resolve(dhtUrl);

		return Networking.bind(dhtEndpoint, new DataServerProtocol() {
			public void onReceive(Envelope src, DHTGetCRDT req) {
				if (logger.isLoggable(Level.INFO)) {
					logger.info("DHT data server: get CRDT : " + req.getId());
				}
				src.reply(new DHTReply(localGetCRDTObject(req)));
			}

			public void onReceive(Envelope src, DHTExecCRDT req) {
				if (logger.isLoggable(Level.INFO)) {
					logger.info("DHT data server: exec CRDT : " + req.getGrp().getTargetUID());
				}
				src.reply(new DHTReply(localExecCRDT(req)));
			}
		});
	}
	/**
	 * Executes operations in the given CRDT
	 * 
	 * @return returns true if the operation could be executed.
	 */
	<V extends CRDT<V>> ExecCRDTResult execCRDT(DHTExecCRDT req) {

		Endpoint dst = DHT_Node.resolveKey(req.getGrp().getTargetUID().toString());
		if (dst == null)
			return localExecCRDT(req);
		else {
			return dhtRequest(dst, req);
		}
	}

	/**
	 * Return null if CRDT does not exist
	 * 
	 * If clock equals to null, just return full CRDT
	 * 
	 * @param subscribe
	 *            Subscription type
	 * @return null if cannot fulfill request
	 */
	ManagedCRDT<?> getCRDT(final CRDTIdentifier id, CausalityClock clk, String clientId, boolean isSubscribed) {
		Endpoint dst = DHT_Node.resolveKey(id.toString());

		DHTGetCRDT request = new DHTGetCRDT(id, clk, clientId, isSubscribed);
		if (dst == null) {
			return localGetCRDTObject(request);
		} else {
			return dhtRequest(dst, request);
		}
	}

	/**
	 */
	<V extends CRDT<V>> CRDTData<V> localPutCRDT(ManagedCRDT<V> crdt) {
		try {
			lock(crdt.getUID());
			@SuppressWarnings("unchecked")
			CRDTData<V> data = (CRDTData<V>) this.storage.getData(crdt.getUID());
			if (data == null) {
				this.storage.putData(data = new CRDTData<V>(crdt));
			} else {
				logger.warning("Unexpected concurrent put of the same object at the DCDataServer");
				Thread.dumpStack();
			}
			return data;
		} finally {
			unlock(crdt.getUID());
		}
	}
	/**
	 * Return null if CRDT does not exist
	 */
	protected ManagedCRDT<?> localGetCRDTObject(DHTGetCRDT req) {
		CRDTIdentifier id = req.getId();

		if (req.subscribesUpdates())
			dsPubSub.subscribe(server.serverId, id, server.suPubSub);

		try {
			lock(id);

			CRDTData<?> data = this.storage.getData(id);
			if (data == null)
				return null;

			// Bandwidth optimization: prune as much as possible before
			// sending.
			final ManagedCRDT<?> crdt = data.copyWithRestrictedVersioning(req.getVersion());

			// TODO CHECK WHY THIS IS NECESSARY...

			// Timestamp ts = clocks.getLatest(clocks.clientClock,
			// req.getCltId());
			// if (ts != null)
			// crdt.augmentWithScoutClockWithoutMappings(ts);

			return crdt;
		} finally {
			unlock(id);
		}
	}

	@SuppressWarnings("unchecked")
	protected <V extends CRDT<V>> ExecCRDTResult localExecCRDT(DHTExecCRDT req) {
		CRDTIdentifier id = req.getGrp().getTargetUID();

		try {
			lock(id);

			CRDTData<V> data = this.storage.getData(id);
			if (data == null) {
				if (!req.getGrp().hasCreationState()) {
					logger.warning("No creation state provided by client for an object that does not exist: " + id);
					return new ExecCRDTResult(false);
				}

				V creationState = (V) req.getGrp().getCreationState();
				final ManagedCRDT<V> crdt = new ManagedCRDT<V>(id, creationState, req.getGrp().getDependency(), true);
				data = localPutCRDT(crdt);
			}

			data.pruneIfPossible(clocks.pruneClockCopy());

			data.execute((CRDTObjectUpdatesGroup<V>) req.getGrp(), CRDTOperationDependencyPolicy.RECORD_BLINDLY);
			if (logger.isLoggable(Level.INFO)) {
				logger.info("Data Server: for crdt : " + id + "; clk = " + data.getClock() + " ; cltClock = "
						+ clocks.clientClockCopy() + ";  snapshotVersion = " + req.getGrp().getDependency()
						+ "; cltTs = " + req.getCltTs());
			}

			ObjectUpdatesInfo info = new ObjectUpdatesInfo(data.getPruneClock().clone(), req.getGrp());

			dsPubSub.publish(new UpdateNotification(req.getCltTs().getIdentifier(), info));

			return new ExecCRDTResult(true, info);
		} finally {
			unlock(id);
		}
	}
	public void updatePruneClock(CausalityClock safeSnapshot) {
		clocks.updateClock(clocks.pruneClock, safeSnapshot);
	}
}
