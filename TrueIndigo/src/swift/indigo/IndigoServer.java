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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.ReturnableTimestampSourceDecorator;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.ManagedCRDT;
import swift.dc.Server;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;
import swift.indigo.proto.AcquireLocksReply;
import swift.indigo.proto.AcquireLocksRequest;
import swift.indigo.proto.FetchObjectReply;
import swift.indigo.proto.FetchObjectRequest;
import swift.indigo.proto.IndigoCommitRequest;
import swift.indigo.proto.ReleaseLocksRequest;
import swift.indigo.remote.RemoteIndigoServer;
import sys.net.api.Endpoint;
import sys.net.api.Envelope;
import sys.net.api.Service;
import sys.utils.Args;
import sys.utils.Threading;
import sys.utils.Timings;

/**
 * Class to handle the Indigo requests from clients.
 * 
 * @author smduarte
 */
public class IndigoServer extends Server {
	AtomicLong snapshots = new AtomicLong(0L);

	static Logger logger = Logger.getLogger(IndigoServer.class.getName());

	final boolean emulateWeakConsistency;
	final boolean emulateRedBlueConsistency;

	Endpoint lockManager;
	AtomicInteger stubCounter = new AtomicInteger(0);

	RemoteIndigoServer rindigo;
	IndigoServer() {
		this.lockManager = super.sequencer;
		this.rindigo = new RemoteIndigoServer(lockManager, this);

		this.emulateWeakConsistency = Args.contains("-weak") || true;
		this.emulateRedBlueConsistency = Args.contains("-redblue") && !emulateWeakConsistency;
	}

	public long registerSnapshot(CausalityClock snapshot) {
		long serial = snapshots.incrementAndGet();
		Snapshots.register(serial, snapshot);
		return serial;
	}

	public void disposeSnapshot(long serial) {
		Snapshots.free(serial);
		super.dataServer.updatePruneClock(Snapshots.safeSnapshot());
	}

	public void onReceive(Envelope src, FetchObjectRequest request) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info("FetchObjectRequest client = " + request.getClientId() + " id: " + request.getUid());
		}

		if (request.hasSubscription())
			getSession(request.getClientId()).subscribe(request.getUid());

		ManagedCRDT<?> crdt = getCRDT(request.getUid(), null, request.getClientId());
		src.reply(new FetchObjectReply(crdt));
	}

	protected Indigo getIndigoInstance() {
		return new IndigoStubImpl(this);
	}

	public static void main(String[] args) {
		Args.use(args);
		new IndigoServer();
	}

	class IndigoStubImpl implements Indigo {

		_TxnHandle handle;
		final IndigoServer server;

		final Service stub;
		final ReturnableTimestampSourceDecorator<Timestamp> tsSource;

		String stubId;
		ClientSession session;

		IndigoStubImpl(IndigoServer server) {
			this.server = server;
			this.stub = server.endpoint4clts;
			this.stubId = server.serverId + "_" + stubCounter.incrementAndGet();
			this.tsSource = new ReturnableTimestampSourceDecorator<Timestamp>(new IncrementalTimestampGenerator(stubId));
			this.session = server.getSession(stubId);
		}
		public String toString() {
			return "indigo-" + stubId;
		}

		public <V extends CRDT<V>> V get(CRDTIdentifier id) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
			return get(id, false, null);
		}

		public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException {
			return (V) handle.get(id, create, classOfV);
		}

		public void beginTxn(Lock... locks) {
			beginTxn(locks, null);
		}

		public void beginTxn(CounterReservation... counters) {
			beginTxn(null, counters);
		}

		public void beginTxn(Lock[] locks, CounterReservation[] counters) {
			try {
				Timings.mark();
				Timestamp txnTimestamp = tsSource.generateNew();

				if (emulateWeakConsistency) {
					handle = new _TxnHandle(txnTimestamp);
					return;
				}

				if ((locks != null && locks.length > 0) || (counters != null && counters.length > 0)) {
					AcquireLocksRequest request = new AcquireLocksRequest(stubId, txnTimestamp, locks, counters);
					for (int delay = 50;; delay = Math.min(1000, 2 * delay)) {
						AcquireLocksReply reply = stub.request(lockManager, request);
						if (reply != null && reply.acquiredLocksAndCounters()) {
							handle = new _TxnHandle(reply, request.cltTimestamp());
							break;
						} else
							Threading.sleep(delay);
					}
				} else
					handle = new _TxnHandle(txnTimestamp);
			} finally {
				Timings.sample("beginTxn");
			}
		}

		public void endTxn() {
			handle.commit();
			disposeSnapshot(handle.serial);
			handle = null;
		}

		public void abortTxn() {
			Thread.dumpStack();
			handle.rollback();
			disposeSnapshot(handle.serial);
			handle = null;
		}

		class _TxnHandle extends AbstractTxHandle {
			final long serial;

			boolean withLocks;
			Timestamp timestamp;

			_TxnHandle(AcquireLocksReply reply, Timestamp cltTimestamp) {
				super(reply.getSnapshot(), cltTimestamp);
				this.timestamp = reply.timestamp();
				this.withLocks = true;

				for (CRDTObjectUpdatesGroup<?> i : reply.lockOps())
					super.ops.put(i.getTargetUID(), i);

				Snapshots.register(serial = snapshots.incrementAndGet(), super.snapshot);
			}

			_TxnHandle(Timestamp cltTimestamp) {
				super(server.clocks.clientClockCopy(), cltTimestamp);
				this.withLocks = false;

				Snapshots.register(serial = snapshots.incrementAndGet(), super.snapshot);
			}

			public void rollback() {
				if (withLocks)
					stub.send(lockManager, new ReleaseLocksRequest(serial, stubId, cltTimestamp()));

				tsSource.returnLastTimestamp();

			}

			@Override
			public void commit() {
				try {
					if (isReadOnly())
						rollback();
					else {
						List<CRDTObjectUpdatesGroup<?>> groups = new ArrayList<CRDTObjectUpdatesGroup<?>>();
						groups.addAll(ops.values());

						final IndigoCommitRequest req = new IndigoCommitRequest(serial, stubId, cltTimestamp(), snapshot, groups);

						if (groups.isEmpty()) {
							rollback();
							return;
						}
						if (timestamp == null) {
							server.prepareAndDoCommit(session, req);
						} else {
							req.setTimestamps(timestamp, null);
							server.doOneCommit(session, req);
						}
					}
				} finally {
				}
			}

			@SuppressWarnings("unchecked")
			@Override
			protected <V extends CRDT<V>> ManagedCRDT<V> getCRDT(CRDTIdentifier id, CausalityClock version, boolean create, Class<V> classOfV) {
				try {
					Timings.mark();
					ManagedCRDT<V> res = (ManagedCRDT<V>) server.getCRDT(id, version, stubId);
					if (res == null && create)
						res = createCRDT(id, server.clocks.currentClockCopy(), classOfV);
					return res;
				} finally {
					Timings.sample("get " + id);
				}
			}
		}
	}
}

class Snapshots {

	static void register(long serial, CausalityClock cc) {
		synchronized (serials) {
			serials.put(serial, cc);
		}
	}

	static CausalityClock safeSnapshot() {
		synchronized (serials) {
			// System.out.println("------------------------>$$$$$$$$$$$" + last
			// + "/" + serials.size());
			if (serials.isEmpty())
				return last;
			else {
				last = serials.get(serials.firstKey());
				return last;
			}
		}
	}

	static void free(long serial) {
		synchronized (serials) {
			CausalityClock cc = serials.remove(serial);
			if (serials.isEmpty() && cc != null)
				last = cc;
		}
	}

	static CausalityClock last = ClockFactory.newClock();
	static SortedMap<Long, CausalityClock> serials = new TreeMap<Long, CausalityClock>();
}