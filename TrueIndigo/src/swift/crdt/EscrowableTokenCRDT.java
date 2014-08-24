/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package swift.crdt;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import swift.api.CRDTIdentifier;
import swift.api.TxnHandle;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;
import swift.indigo.Resource;
import swift.indigo.ResourceRequest;
import swift.indigo.TRANSFER_STATUS;
import swift.utils.Pair;

public class EscrowableTokenCRDT extends BaseCRDT<EscrowableTokenCRDT> implements Resource<ShareableLock> {
	private static Logger Log = Logger.getLogger(EscrowableTokenCRDT.class.getName());

	private ShareableLock type;
	private Map<String, Set<TripleTimestamp>> owners;

	// TODO: Does it need to track WRITE_EXCLUSIVE versions? apparently not.

	// For kryo
	public EscrowableTokenCRDT() {
	}

	public EscrowableTokenCRDT(CRDTIdentifier id) {
		super(id);
		this.type = ShareableLock.getDefault();
		this.owners = new HashMap<String, Set<TripleTimestamp>>();
	}

	public EscrowableTokenCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, ShareableLock type,
			Map<String, Set<TripleTimestamp>> owners) {
		super(id, txn, clock);
		this.type = type;
		this.owners = owners;
	}

	@Override
	public ShareableLock getValue() {
		return type;
	}

	@Override
	public EscrowableTokenCRDT copy() {
		Map<String, Set<TripleTimestamp>> ownersCopy = new HashMap<String, Set<TripleTimestamp>>();
		for (Entry<String, Set<TripleTimestamp>> entry : owners.entrySet()) {
			ownersCopy.put(entry.getKey(), new HashSet<TripleTimestamp>(entry.getValue()));
		}
		return new EscrowableTokenCRDT(id, txn, clock, type, ownersCopy);
	}

	public boolean alreadyUpdated(String requesterId, ShareableLock requestedType) {
		return (isOwner(requesterId) && requestedType.isShareable());
	}

	public boolean isOwner(String ownerId) {
		return owners.isEmpty() || owners.containsKey(ownerId);
	}

	public boolean isSingleOwner(String ownerId) {
		return owners.isEmpty() || (owners.size() == 1 && owners.containsKey(ownerId));
	}

	// Owns a shared-lock
	protected boolean canShare(String ownerId, ShareableLock lockType) {
		return type.isShareable() && type.equals(lockType) && isOwner(ownerId);
	}

	// Can grant exclusive lock
	protected boolean canGrantExclusive(String ownerId, ShareableLock requestedType) {
		return requestedType.isExclusive() && isSingleOwner(ownerId);
	}

	protected boolean canUpdateSharedLock(String ownerId, ShareableLock requestedType) {
		return canGrantExclusive(ownerId, requestedType) || canShare(ownerId, requestedType) || canMutateLock(ownerId);
	}

	protected boolean canMutateLock(String ownerId) {
		return isSingleOwner(ownerId);
	}

	public TRANSFER_STATUS updateOwnership(String ownerId, String requesterId, ShareableLock requestType) {
		Log.info(String.format("ownerId=%s requesterId=%s, requestType=%s, this=%s, canMudate=%s, canUpdate=%s",
				ownerId, requesterId, requestType, this, canMutateLock(ownerId), canShare(ownerId, requestType)));

		if (canMutateLock(ownerId)) {
			EscrowableLockUpdate op = new EscrowableLockUpdate(ownerId, requesterId, requestType, nextTimestamp(),
					requestType.isExclusive());
			applySharedLockUpdate(op);
			registerLocalOperation(op);
			Log.info("GAVE EXCLUSIVE OWNERSHIP TO: " + requesterId + "NOW: " + this);
			return TRANSFER_STATUS.SUCCESS;
		}

		if (canShare(ownerId, requestType)) {
			boolean isTransfer = requestType.isExclusive();
			EscrowableLockUpdate op = new EscrowableLockUpdate(ownerId, requesterId, this.type, nextTimestamp(),
					isTransfer);
			applySharedLockUpdate(op);
			registerLocalOperation(op);
			Log.info("SHARED " + requestType + " WITH: " + requesterId + " NOW:" + this);
			return TRANSFER_STATUS.SUCCESS;
		}
		return TRANSFER_STATUS.FAIL;
	}

	protected void applySharedLockUpdate(EscrowableLockUpdate op) {
		Log.info("SharedLockOwnershipUpdate: " + op);
		// Check if the pre-condition of updateOwnership is still valid
		if (op.justRelease()) {
			owners.remove(op.ownerId());
			return;
		}

		if (!canUpdateSharedLock(op.ownerId(), op.getType()))
			throw new IncompatibleLockException("Can't get ownership on downstream! op: " + op + " current: " + this);

		// TODO: Does underlying layer ensures only once? Otherwise needs to
		// check that the current TS << Request TS. The same for JustRelease
		if (op.isTransfer())
			owners.remove(op.ownerId());

		type = op.getType();

		Set<TripleTimestamp> tsRequester = owners.get(op.requesterId());
		if (tsRequester == null) {
			tsRequester = new HashSet<TripleTimestamp>();
			owners.put(op.requesterId(), tsRequester);

			tsRequester.add(op.getTimestamp());
		}
	}
	public void setTxnHandle(TxnHandle handle) {
		super.txn = handle;
	}

	public Set<String> owners() {
		return new HashSet<String>(owners.keySet());
	}

	public String toString() {
		return String.format("EscrowableCRDT type: %s, owners: %s", type, owners.keySet());
	}

	@Override
	public void initialize(String ownerId, ResourceRequest<ShareableLock> request) {
		updateOwnership(ownerId, request.getRequesterId(), request.getResource());

	}

	@Override
	public TRANSFER_STATUS transferOwnership(String fromId, String toId, ResourceRequest<ShareableLock> request) {
		return updateOwnership(fromId, toId, request.getResource());
	}

	@Override
	public boolean checkRequest(String ownerId, ResourceRequest<ShareableLock> request) {
		return canUpdateSharedLock(ownerId, request.getResource());
	}

	@Override
	public void apply(String siteId, ResourceRequest<ShareableLock> request) {
		updateOwnership(siteId, request.getRequesterId(), request.getResource());
	}

	@Override
	public ShareableLock getCurrentResource() {
		return type;
	}

	@Override
	public ShareableLock getSiteResource(String siteId) {
		if (isOwner(siteId)) {
			return getCurrentResource();
		} else {
			return ShareableLock.NONE;
		}

	}

	@Override
	public boolean isReservable() {
		return true;
	}

	@Override
	public Queue<Pair<String, ShareableLock>> preferenceList(String excludeSiteId) {
		List<Pair<String, ShareableLock>> list = new LinkedList<>();
		for (String entry : owners.keySet()) {
			if (excludeSiteId != null && !excludeSiteId.equals(entry))
				list.add(new Pair<String, ShareableLock>(entry, type));
		}
		return (Queue<Pair<String, ShareableLock>>) list;
	}

	@Override
	public Queue<Pair<String, ShareableLock>> preferenceList() {
		List<Pair<String, ShareableLock>> list = new LinkedList<>();
		for (String entry : owners.keySet()) {
			list.add(new Pair<String, ShareableLock>(entry, type));
		}
		return (Queue<Pair<String, ShareableLock>>) list;
	}

	@Override
	public Collection<String> getAllResourceOwners() {
		return owners.keySet();
	}

	/**
	 * Releases the token of ownerId, by transferring it to the lowest index
	 * node. If node is the lowest index ordering it does not release its share
	 * to make sure there is one owner if everyone tries to release
	 */
	@Override
	public boolean releaseShare(String ownerId) {
		PriorityQueue<String> orderedOwners = new PriorityQueue<>(owners.keySet());
		if (orderedOwners.size() > 0 && !orderedOwners.peek().equals(ownerId)) {
			EscrowableLockUpdate op = new EscrowableLockUpdate(ownerId, nextTimestamp(), true);
			applySharedLockUpdate(op);
			registerLocalOperation(op);
			return true;
		}
		return false;
	}
}
