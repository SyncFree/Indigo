package swift.indigo;

import swift.api.CRDT;
import swift.api.CRDTIdentifier;
import swift.exceptions.NetworkException;
import swift.exceptions.NoSuchObjectException;
import swift.exceptions.VersionNotFoundException;
import swift.exceptions.WrongTypeException;

public interface Indigo {

	public void beginTxn(Lock... locks);

	public void beginTxn(Lock[] locks, CounterReservation[] counters);

	public void endTxn();

	public <V extends CRDT<V>> V get(CRDTIdentifier id) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

	public <V extends CRDT<V>> V get(CRDTIdentifier id, boolean create, Class<V> classOfV) throws WrongTypeException, NoSuchObjectException, VersionNotFoundException, NetworkException;

	public void abortTxn();
}
