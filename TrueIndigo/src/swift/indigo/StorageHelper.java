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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ReturnableTimestampSourceDecorator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampSource;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.SwiftException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.indigo.proto.FetchObjectReply;
import swift.indigo.proto.FetchObjectRequest;
import swift.proto.CommitUpdatesReply;
import swift.proto.CommitUpdatesRequest;
import sys.net.api.Endpoint;
import sys.net.api.Service;
import sys.utils.Timings;

/*
 * TODO: Clock versions should be handled somewhere else.
 * TODO: Put cache here?
 */
public class StorageHelper {
	private static Logger Log = Logger.getLogger(StorageHelper.class.getName());

	final String LOCK_MANAGER;
	final Service stub;
	final IndigoSequencer sequencer;

	private final CausalityClock grantedRequests;
	private final Map<Class, Class> tableToType;
	private final boolean isMasterLockManager;
	private final Endpoint surrogate;

	private final ReturnableTimestampSourceDecorator<Timestamp> dummyTsSource;

	// TODO: Sequencer should be remote
	public StorageHelper(final IndigoSequencer sequencer, Endpoint surrogate, String resourceMgrId,
			boolean isMasterLockManager, ReturnableTimestampSourceDecorator<Timestamp> dummyTsSource) {
		this.sequencer = sequencer;
		this.LOCK_MANAGER = sequencer.siteId + "_LockManager";
		this.stub = sequencer.stub;
		this.isMasterLockManager = isMasterLockManager;
		this.surrogate = surrogate;
		this.dummyTsSource = dummyTsSource;
		this.grantedRequests = getCurrentClockCopy();
		this.tableToType = new HashMap<>();
	}

	public <A extends ResourceRequest<?>, B extends CRDT<?>> void registerType(Class<A> requestType, Class<B> theClass) {
		this.tableToType.put(requestType, theClass);
	}

	CausalityClock getCurrentClockCopy() {
		return sequencer.clocks().currentClockCopy();
	}

	public CausalityClock getLocalSnapshotClockCopy() {
		return grantedRequests.clone();
	}

	_TxnHandle beginTxn(Timestamp cltTimestamp, TimestampSource<Timestamp> tsSourceForTxn) {
		return new _TxnHandle(getCurrentClockCopy(), cltTimestamp, tsSourceForTxn);
	}

	public void endTxn(final _TxnHandle handle, final boolean writeThrough) {
		if (writeThrough)
			handle.commit();
	}

	public ManagedCRDT<?> getResource(ResourceRequest<?> req, final _TxnHandle handle) throws SwiftException {
		Class<CRDT> type = tableToType.get(req.getClass());
		return handle.getLatestVersion(req.getResourceId(), isMasterLockManager, type);
	}

	class _TxnHandle extends AbstractTxHandle {

		protected Timestamp commitTs;
		protected CausalityClock lockManagerAndSnapshot;
		protected TimestampSource<Timestamp> tsSourceForTxn;

		_TxnHandle(CausalityClock snapshot, Timestamp cltTimestamp, TimestampSource<Timestamp> tsSourceForTxn) {
			super(snapshot, cltTimestamp);
			synchronized (grantedRequests) {
				grantedRequests.merge(snapshot);
				this.lockManagerAndSnapshot = grantedRequests.clone();
				this.tsSourceForTxn = tsSourceForTxn;
			}
		}

		@Override
		public void commit() {
			try {

				Timings.mark();
				final CommitUpdatesRequest req;
				List<CRDTObjectUpdatesGroup<?>> updates = getUpdates();

				if (!updates.isEmpty()) {
					// commitTs = sequencer.clocks().newTimestamp();

					// for (CRDTObjectUpdatesGroup<?> upd : updates) {
					// upd.addSystemTimestamp(commitTs);
					// }

					req = new CommitUpdatesRequest(cltTimestamp().getIdentifier(), cltTimestamp(), snapshot, updates);
					// req.setTimestamp(commitTs);
				} else {
					req = new CommitUpdatesRequest(commitTs.getIdentifier(), new Timestamp("dummy", -1L), snapshot,
							updates);
					Thread.dumpStack();
					System.exit(0);
				}

				final Semaphore semaphore = new Semaphore(0);
				if (Log.isLoggable(Level.INFO)) {
					Log.info("GOING TO COMMIT------->>" + req + " FOR : " + req.getTimestamp());
				}
				stub.asyncRequest(surrogate, req, (CommitUpdatesReply r) -> {
					if (Log.isLoggable(Level.INFO)) {
						Log.info("FINISH COMMIT------->>" + r.getStatus() + " FOR : " + req.getTimestamp());
					}
					semaphore.release();
				});
				semaphore.acquireUninterruptibly();
			} finally {
				Timings.sample("async commit");
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		protected <V extends CRDT<V>> ManagedCRDT<V> getCRDT(CRDTIdentifier uid, CausalityClock version,
				boolean create, Class<V> classOfV) throws VersionNotFoundException {
			FetchObjectRequest req = new FetchObjectRequest(grantedRequests, version, LOCK_MANAGER, uid, true);

			FetchObjectReply reply = stub.request(surrogate, req);

			if (reply != null) {
				if (reply.getStatus() == FetchObjectReply.FetchStatus.OK) {
					ManagedCRDT<V> res = (ManagedCRDT<V>) reply.getCrdt();
					CMP_CLOCK cmp = version.compareTo(res.getClock());
					if (cmp.is(CMP_CLOCK.CMP_EQUALS, CMP_CLOCK.CMP_ISDOMINATED))
						return res;
					else {
						throw new VersionNotFoundException("Version not found: " + version);
					}
				}
				if (create && reply.getStatus() == FetchObjectReply.FetchStatus.OBJECT_NOT_FOUND) {
					return createCRDT(uid, version, classOfV);
				}
			}
			return null;
		}

		@SuppressWarnings("unchecked")
		public <V extends CRDT<V>> ManagedCRDT<V> getLatestVersion(CRDTIdentifier id, boolean create, Class<V> classOfV)
				throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {

			FetchObjectRequest req = new FetchObjectRequest(getCurrentClockCopy(), getLocalSnapshotClockCopy(),
					LOCK_MANAGER, id, true);
			FetchObjectReply reply = stub.request(surrogate, req);

			if (reply != null) {
				if (reply.getStatus() == FetchObjectReply.FetchStatus.OK) {
					ManagedCRDT<V> res = (ManagedCRDT<V>) reply.getCrdt();
					return res;
				}
				if (create && reply.getStatus() == FetchObjectReply.FetchStatus.OBJECT_NOT_FOUND) {
					return createCRDT(id, getCurrentClockCopy(), classOfV);
				}
			}
			return null;
		}

		@Override
		protected Timestamp cltTimestamp() {
			if (cltTimestamp == null) {
				// verificar se o acquire tambem passa por aqui. se sim, tirar o
				// record new event do acquire.
				cltTimestamp = recordNewEvent(tsSourceForTxn);

			}

			return cltTimestamp;
		}
	}

	public Timestamp recordNewEvent(TimestampSource<Timestamp> tsSource) {
		Timestamp ts = tsSource.generateNew();
		// sequencer, is it
		// safe????
		if (ts != null)
			synchronized (grantedRequests) {
				grantedRequests.record(ts);
			}
		return ts;
	}

	public Collection<CRDTObjectUpdatesGroup<?>> endTxnAndGetUpdates(final _TxnHandle handle, boolean b) {
		endTxn(handle, b);
		return handle.getUpdates();
	}

	public void setNewClientTS(_TxnHandle handle, Timestamp newTimestamp) {
		for (Entry<CRDTIdentifier, CRDTObjectUpdatesGroup<?>> entry : handle.ops.entrySet())
			entry.getValue().substituteClientTimestamp(newTimestamp);
		System.out.println("After update client timestamp " + handle.ops);
	}

}
