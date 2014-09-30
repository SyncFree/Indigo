package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;

/**
 * By design this class returns the values of the original Resource minus the
 * resources being used. Must think if this is the best solution
 * 
 * @author balegas
 * 
 * @param <V>
 */

// TODO: It seems the TokenCRDT does not properlly handles the case of mutating
// from a shared allow to a shared forbid
public class LocalLock {

	private Set<Timestamp> activeClients;
	private Map<Timestamp, CausalityClock> clocks;
	private ShareableLock nextType;

	public LocalLock() {
		this.activeClients = new HashSet<>();
		this.clocks = new HashMap<>();
	}

	public void lock(Timestamp cltTs, ShareableLock type, CausalityClock snapshot) {
		if (checkAvailable(type)) {
			activeClients.add(cltTs);
			clocks.put(cltTs, snapshot);
			if (nextType != null && type.equals(nextType)) {
				// System.out.println("satisfied next "+nextType);
				nextType = null;
			}
		}
	}

	public boolean checkCanRelease() {
		return activeClients.isEmpty();
	}
	public boolean checkAvailable(ShareableLock otherType) {
		if (nextType == null || otherType.compareTo(nextType) < 0) {
			nextType = otherType;
		}

		return (nextType != null && nextType.equals(otherType)) && activeClients.isEmpty() || (otherType.isShareable());
	}
	public boolean release(String siteId, Timestamp cltTs) {
		if (!activeClients.remove(cltTs)) {
			System.out.println("ERROR release did not remove a resource " + cltTs);
			System.exit(0);
		}
		clocks.remove(cltTs);
		return true;
	}

	public String toString() {
		return "ACTIVE: " + activeClients.size() + " NEXT: " + nextType + " HOLDING: " + clocks;
	}
}
