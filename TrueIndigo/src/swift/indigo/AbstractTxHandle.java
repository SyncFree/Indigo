package swift.indigo;

import static sys.utils.NotImplemented.NotImplemented;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import swift.api.BulkGetProgressListener;
import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.api.CommitListener;
import swift.api.ObjectUpdatesListener;
import swift.api.TxnHandle;
import swift.clocks.CausalityClock;
import swift.clocks.IncrementalTripleTimestampGenerator;
import swift.clocks.Timestamp;
import swift.clocks.TimestampMapping;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import swift.crdt.core.CRDTUpdate;
import swift.crdt.core.ManagedCRDT;
import swift.crdt.core.TxnStatus;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

abstract public class AbstractTxHandle implements TxnHandle {

	protected TxnStatus status;
	public Timestamp cltTimestamp;
	protected CausalityClock snapshot;
	protected Map<CRDTIdentifier, CRDTObjectUpdatesGroup<?>> ops;

	protected AbstractTxHandle(CausalityClock snapshot, Timestamp cltTimestamp) {
		this.ops = new HashMap<CRDTIdentifier, CRDTObjectUpdatesGroup<?>>();
		this.snapshot = snapshot.clone();
		this.cltTimestamp = cltTimestamp;
		this.status = TxnStatus.PENDING;
	}

	protected abstract <V extends CRDT<V>> ManagedCRDT<V> getCRDT(CRDTIdentifier id, CausalityClock version,
			boolean create, Class<V> classOfV) throws VersionNotFoundException;

	@SuppressWarnings("unchecked")
	@Override
	public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV) throws WrongTypeException,
			NoSuchObjectException, VersionNotFoundException, NetworkException {

		V res = (V) cache.get(id);
		if (res == null)
			cache.put(id, res = this.getCRDT(id, snapshot, create, classOfV).getVersion(snapshot, this));

		return res;
	}

	@Override
	public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV,
			ObjectUpdatesListener updatesListener) throws WrongTypeException, NoSuchObjectException,
			VersionNotFoundException, NetworkException {
		return get(id, create, classOfV);
	}

	@Override
	public void commitAsync(CommitListener listener) {
		commit();
	}

	@Override
	public void commit() {
		throw NotImplemented;
	}

	@Override
	public void rollback() {
		throw NotImplemented;
	}

	@Override
	public TxnStatus getStatus() {
		return status;
	}

	protected <V extends CRDT<V>> ManagedCRDT<V> createCRDT(CRDTIdentifier id, CausalityClock clock, Class<V> classOfV) {
		try {
			final Constructor<V> constructor = classOfV.getConstructor(CRDTIdentifier.class);
			final V checkpoint = constructor.newInstance(id);
			return new ManagedCRDT<V>(id, checkpoint, clock, false);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public TripleTimestamp nextTimestamp() {
		return tsSource().generateNew();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V extends CRDT<V>> void registerOperation(CRDTIdentifier id, CRDTUpdate<V> op) {
		synchronized (this) {
			CRDTObjectUpdatesGroup<V> group = (CRDTObjectUpdatesGroup<V>) ops.get(id);
			if (group == null)
				ops.put(id, group = new CRDTObjectUpdatesGroup<V>(id, timestampMapping(), null, snapshot));

			group.append(op);
		}
	}

	@Override
	public <V extends CRDT<V>> void registerObjectCreation(CRDTIdentifier id, V creationState) {
		synchronized (this) {
			CRDTObjectUpdatesGroup<V> group = new CRDTObjectUpdatesGroup<V>(id, timestampMapping(), creationState,
					snapshot);
			if (ops.put(id, group) != null) {
				throw new IllegalStateException("CRDT creation was preceded by some other operation:" + id);
			}
		}
	}

	public List<CRDTObjectUpdatesGroup<?>> getUpdates() {
		List<CRDTObjectUpdatesGroup<?>> groups = new ArrayList<CRDTObjectUpdatesGroup<?>>();
		groups.addAll(ops.values());
		return groups;
	}

	public boolean isReadOnly() {
		return ops.isEmpty();
	}

	protected TimestampMapping timestampMapping() {
		if (timestampMapping == null)
			timestampMapping = new TimestampMapping(cltTimestamp());
		return timestampMapping;
	}

	private IncrementalTripleTimestampGenerator tsSource() {
		if (tsSource == null)
			tsSource = new IncrementalTripleTimestampGenerator(cltTimestamp());
		return tsSource;
	}

	protected Timestamp cltTimestamp() {
		return cltTimestamp;
	}

	private TimestampMapping timestampMapping;
	private IncrementalTripleTimestampGenerator tsSource;

	protected Map<CRDTIdentifier, CRDT<?>> cache = new HashMap<CRDTIdentifier, CRDT<?>>();

	@Override
	public Map<CRDTIdentifier, CRDT<?>> bulkGet(boolean subscribeUpdates, final Set<CRDTIdentifier> ids,
			final BulkGetProgressListener listener) {
		return bulkGet(subscribeUpdates, ids.toArray(new CRDTIdentifier[ids.size()]));
	}

	/**
	 * TODO document
	 * 
	 * @param ids
	 * @return
	 */
	public Map<CRDTIdentifier, CRDT<?>> bulkGet(boolean subscribeUpdates, final CRDTIdentifier... ids) {
		Map<CRDTIdentifier, CRDT<?>> res = new HashMap<CRDTIdentifier, CRDT<?>>();
		for (CRDTIdentifier i : ids)
			try {
				res.put(i, get(i, false, null));
			} catch (Exception x) {
				x.printStackTrace();
			}
		return res;
	}

	public String toString() {
		return "CLT " + cltTimestamp + " SNAPSHOT " + snapshot + " OPS " + ops.size();
	}
}
