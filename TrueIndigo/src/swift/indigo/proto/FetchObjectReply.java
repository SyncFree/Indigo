package swift.indigo.proto;

import swift.crdt.core.ManagedCRDT;

/**
 * Server reply to object version fetch request.
 * 
 * @author smduarte
 */
public class FetchObjectReply {

	public enum FetchStatus {
		/**
		 * The reply contains requested version.
		 */
		OK,
		/**
		 * The requested object is not in the store.
		 */
		OBJECT_NOT_FOUND,
	}

	protected FetchStatus status;
	protected ManagedCRDT<?> crdt;

	public FetchObjectReply() {
	}

	public FetchObjectReply(ManagedCRDT<?> crdt) {
		this.crdt = crdt;
		this.status = crdt == null ? FetchStatus.OBJECT_NOT_FOUND : FetchStatus.OK;
	}

	/**
	 * @return status code of the reply
	 */
	public FetchStatus getStatus() {
		return status;
	}

	/**
	 * @return state of an object requested by the client; null if
	 *         {@link #getStatus()} is {@link FetchStatus#OBJECT_NOT_FOUND}.
	 */

	public ManagedCRDT<?> getCrdt() {
		return crdt;
	}
}
