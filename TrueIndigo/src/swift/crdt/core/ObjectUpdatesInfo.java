package swift.crdt.core;

import swift.api.CRDTIdentifier;
import swift.clocks.CausalityClock;
import sys.KryoLib;

public class ObjectUpdatesInfo {

	protected CausalityClock pruneClock;
	protected CRDTObjectUpdatesGroup<?> update;

	public ObjectUpdatesInfo() {
	}

	public ObjectUpdatesInfo(CausalityClock pruneClock, CRDTObjectUpdatesGroup<?> update) {
		this.update = update;
		this.pruneClock = pruneClock;
	}

	/**
	 * @return id of the object
	 */
	public CRDTIdentifier getId() {
		return update.id;
	}

	public ObjectUpdatesInfo clone() {
		return KryoLib.copy(this);
	}

	public CausalityClock getPruneClock() {
		return pruneClock;
	}
}
