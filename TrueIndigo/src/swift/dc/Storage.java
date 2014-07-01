package swift.dc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;

public class Storage {

	Map<String, Map<String, CRDTData<?>>> db;

	Storage() {
		db = new ConcurrentHashMap<String, Map<String, CRDTData<?>>>();
	}
	/**
	 * Returns database entry, if it exists.
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <V extends CRDT<V>> CRDTData<V> getData(CRDTIdentifier uid) {
		Map<String, CRDTData<?>> m = db.get(uid.getTable()), nm;
		if (m == null) {
			m = db.putIfAbsent(uid.getTable(), nm = new ConcurrentHashMap<String, CRDTData<?>>());
			if (m == null)
				m = nm;
		}

		CRDTData<V> data = (CRDTData<V>) m.get(uid.getKey());
		if (data != null)
			return data;
		else
			return readCRDTFromDB(uid);

	}

	public <V extends CRDT<V>> void putData(CRDTData<V> data) {
		CRDTIdentifier uid = data.getUID();
		CRDTData<V> oldData = getData(uid);
		synchronized (db) {
			if (oldData == null)
				db.get(uid.getTable()).put(uid.getKey(), data);
			else {
				oldData.merge(data);
			}
		}
	}

	private <V extends CRDT<V>> CRDTData<V> readCRDTFromDB(CRDTIdentifier uid) {
		return null;
	}

}
