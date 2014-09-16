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
	private ShareableLock type;

	public LocalLock() {
		super();
	}

	public LocalLock(ShareableLock type) {
		this.activeClients = new HashSet<ResourceRequest<ShareableLock>>();
		this.type = type;
	}

	public void lock(ResourceRequest<ShareableLock> req) {
		if (checkAvailable(req)) {
			activeClients.add(req);
			type = req.getResource();
		} else {
			System.out.println("LOCK but not available " + req);
			System.exit(0);
		}
	}
	public boolean checkAvailable(ResourceRequest<ShareableLock> request) {
		ShareableLock otherResource = request.getResource();
		return activeClients.isEmpty() || (otherResource.isShareable() && type.equals(otherResource));
	}

	public boolean release(String siteId, ResourceRequest<ShareableLock> req) {
		if (!activeClients.remove(req)) {
			System.out.println("ERROR release did not remove a resource " + req);
			System.exit(0);
		}
		if (!req.getResource().equals(type)) {
			System.out.println("ERROR release different types" + req);
			System.exit(0);
		}
		return true;
	}

	public void updateType(ShareableLock newType) {
		if (!newType.equals(type) && !activeClients.isEmpty()) {
			System.out.println("Storage has a different type but still locked (database is older?)");
			// System.exit(0);
		} else {
			this.type = newType;
		}

	}

	public String toString() {
		return type + " " + activeClients;
	}
}
