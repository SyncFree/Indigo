package swift.crdt;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import swift.api.CRDTIdentifier;
import swift.exceptions.IncompatibleTypeException;
import swift.indigo.Resource;
import swift.indigo.ResourceDecorator;
import swift.indigo.ResourceRequest;
import swift.indigo.TRANSFER_STATUS;
import swift.utils.Pair;

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
public class EscrowableTokenCRDTWithLocks extends ResourceDecorator<EscrowableTokenCRDTWithLocks, ShareableLock> {

	private String localId;
	private Set<ResourceRequest<ShareableLock>> activeClients;
	private EscrowableTokenCRDT token;

	public EscrowableTokenCRDTWithLocks() {
		super();
	}

	public EscrowableTokenCRDTWithLocks(String localId, CRDTIdentifier uid, EscrowableTokenCRDT original) {
		super(uid, original);
		this.localId = localId;
		this.activeClients = new HashSet<ResourceRequest<ShareableLock>>();
		this.token = original;
	}

	private EscrowableTokenCRDTWithLocks(String localId, CRDTIdentifier uid, EscrowableTokenCRDT resource,
			Set<ResourceRequest<ShareableLock>> activeClients) {
		this(localId, uid, resource);
		this.activeClients = activeClients;
		this.token = resource;
	}

	@Override
	public void initialize(String ownerId, ResourceRequest<ShareableLock> request) {
		super.initialize(ownerId, request);
	}

	@Override
	public void apply(String siteId, ResourceRequest<ShareableLock> req) {
		TRANSFER_STATUS success = transferOwnership(siteId, req.getRequesterId(), req);
		activeClients.add(req);
	}

	protected boolean canMutateLock(String ownerId) {
		return token.canMutateLock(ownerId) && activeClients.isEmpty();
	}

	protected boolean canShare(String ownerId, ShareableLock lockType) {
		return token.canShare(ownerId, lockType);
	}

	protected boolean canGrantExclusive(String ownerId, ShareableLock requestedType) {
		return token.canGrantExclusive(ownerId, requestedType) && activeClients.isEmpty();
	}

	protected boolean canUpdateSharedLock(String ownerId, ShareableLock requestedType) {
		return canGrantExclusive(ownerId, requestedType) || canShare(ownerId, requestedType) || canMutateLock(ownerId);
	}

	@Override
	public boolean checkRequest(String ownerId, ResourceRequest<ShareableLock> request) {
		return canUpdateSharedLock(ownerId, request.getResource())
				|| (activeClients.isEmpty() && token.isSingleOwner(ownerId));
	}

	@Override
	public TRANSFER_STATUS transferOwnership(String fromId, String toId, ResourceRequest<ShareableLock> request) {
		if (checkRequest(fromId, request)) {
			TRANSFER_STATUS success = super.transferOwnership(fromId, toId, request);
			if (success != TRANSFER_STATUS.SUCCESS) {
				System.err.println("BUG -  SHOULD SUCCEED - REMOVE THIS MESSAGE");
				System.exit(0);
			}
			return success;
		}
		return TRANSFER_STATUS.FAIL;
	}

	@Override
	public EscrowableTokenCRDTWithLocks createDecoratorCopy(Resource<ShareableLock> resource)
			throws IncompatibleTypeException {
		if (resource instanceof EscrowableTokenCRDT) {
			EscrowableTokenCRDTWithLocks newDecorator = new EscrowableTokenCRDTWithLocks(localId, getUID(),
					(EscrowableTokenCRDT) resource, new HashSet<ResourceRequest<ShareableLock>>(activeClients));
			return newDecorator;
		} else {
			throw new IncompatibleTypeException();
		}
	}

	public static EscrowableTokenCRDTWithLocks createDecorator(String callerId, EscrowableTokenCRDT resource) {
		return new EscrowableTokenCRDTWithLocks(callerId, resource.getUID(), resource);
	}

	public void release(String siteId, ResourceRequest<?> req_i) {
		if (!activeClients.remove(req_i)) {
			System.out.println("ERROR release did not remove a resource " + req_i);
			System.exit(0);
		}
	}

	@Override
	public Queue<Pair<String, ShareableLock>> preferenceList() {
		return token.preferenceList();
	}

	@Override
	public Queue<Pair<String, ShareableLock>> preferenceList(String excludeSiteId) {
		return token.preferenceList(excludeSiteId);
	}

	public String toString() {
		return "State: " + token + " Active: " + activeClients;
	}

	@Override
	public boolean isSingleOwner(String ownerId) {
		return token.isSingleOwner(ownerId);
	}

	@Override
	public boolean releaseShare(String ownerId) {
		if (activeClients.isEmpty()) {
			return token.releaseShare(ownerId);
		} else {
			return false;
		}
	}
}
