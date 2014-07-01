package swift.api;


public interface BulkGetProgressListener {

	void onGet(final TxnHandle txn, final CRDTIdentifier id, CRDT<?> view);

}
