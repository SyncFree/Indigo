package swift.indigo;

import swift.api.CRDTIdentifier;
import swift.clocks.Timestamp;

public interface ResourceRequest<T> extends Comparable<ResourceRequest<T>> {
	public T getResource();

	public CRDTIdentifier getResourceId();

	public String getRequesterId();

	void setClientTs(Timestamp ts);

	public String key();

}
