package swift.crdt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import swift.clocks.CausalityClock;
import swift.indigo.ResourceRequest;

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

	private Set<ResourceRequest<ShareableLock>> activeClients;
	private Map<String, CausalityClock> clocks;
	private ShareableLock nextType;

	public LocalLock() {
		this.activeClients = new HashSet<>();
		this.clocks = new HashMap<>();
	}

	public void lock(ResourceRequest<ShareableLock> req, CausalityClock snapshot) {
		if (checkAvailable(req)) {
			activeClients.add(req);
			clocks.put(req.getRequesterId(), snapshot);
			if (nextType != null && req.getResource().equals(nextType)) {
				// System.out.println("satisfied next "+nextType);
				nextType = null;
			}
		} else {
			System.out.println("LOCK but not available " + req);
			System.exit(0);
		}
	}

	public boolean checkCanRelease() {
		return activeClients.isEmpty();
	}
	public boolean checkAvailable(ResourceRequest<ShareableLock> request) {
		ShareableLock otherResource = request.getResource();
		if (nextType == null || request.getResource().compareTo(nextType) < 0) {
			nextType = request.getResource();
		}

		return (nextType != null && nextType.equals(request.getResource())) && activeClients.isEmpty() || (otherResource.isShareable());
	}
	public boolean release(String siteId, ResourceRequest<ShareableLock> req) {
		if (!activeClients.remove(req)) {
			System.out.println("ERROR release did not remove a resource " + req);
			System.exit(0);
		}
		clocks.remove(req.getRequesterId());
		return true;
	}

	public String toString() {
		return "ACTIVE: " + activeClients.size() + " NEXT: " + nextType + " HOLDING: " + clocks;
	}
}
