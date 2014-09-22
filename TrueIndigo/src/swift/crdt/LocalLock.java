package swift.crdt;

import java.util.HashSet;
import java.util.Set;

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

	public LocalLock() {
		super();
	}

	public LocalLock(ShareableLock type) {
		this.activeClients = new HashSet<ResourceRequest<ShareableLock>>();
	}

	public void lock(ResourceRequest<ShareableLock> req) {
		if (checkAvailable(req)) {
			activeClients.add(req);
		} else {
			System.out.println("LOCK but not available " + req);
			System.exit(0);
		}
	}
	public boolean checkAvailable(ResourceRequest<ShareableLock> request) {
		ShareableLock otherResource = request.getResource();
		return activeClients.isEmpty() || (otherResource.isShareable());
	}

	public boolean release(String siteId, ResourceRequest<ShareableLock> req) {
		if (!activeClients.remove(req)) {
			System.out.println("ERROR release did not remove a resource " + req);
			System.exit(0);
		}
		return true;
	}

	public String toString() {
		return activeClients.toString();
	}
}
