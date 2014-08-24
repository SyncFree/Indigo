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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import swift.api.CRDTIdentifier;
import swift.api.TxnHandle;
import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.clocks.TripleTimestamp;
import swift.crdt.core.BaseCRDT;

public class SharedLockCRDT extends BaseCRDT<SharedLockCRDT> {
	private static Logger Log = Logger.getLogger(SharedLockCRDT.class.getName());

	private LockType type;
	private Map<String, Set<TripleTimestamp>> owners;
	transient private Set<Timestamp> users;
	transient private boolean outdated;

	// TODO: Does it need to track WRITE_EXCLUSIVE versions? apparently not.

	// For kryo
	public SharedLockCRDT() {
		users = new HashSet<Timestamp>();
	}

	public SharedLockCRDT(CRDTIdentifier id) {
		this(id, null);
	}

	public SharedLockCRDT(CRDTIdentifier id, String owner) {
		super(id);
		this.type = LockType.ALLOW;
		this.users = new HashSet<Timestamp>();
		this.owners = new HashMap<String, Set<TripleTimestamp>>();
	}

	public SharedLockCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, LockType type,
			Map<String, Set<TripleTimestamp>> owners) {
		super(id, txn, clock);
		this.type = type;
		this.owners = owners;
		this.users = new HashSet<Timestamp>();
	}

	@Override
	public LockType getValue() {
		return type;
	}

	@Override
	public SharedLockCRDT copy() {
		Map<String, Set<TripleTimestamp>> ownersCopy = new HashMap<String, Set<TripleTimestamp>>();
		owners.forEach((k, v) -> {
			ownersCopy.put(k, new HashSet<TripleTimestamp>(v));
		});
		return new SharedLockCRDT(id, txn, clock, type, ownersCopy);
	}

	public boolean alreadyUpdated(String requesterId, LockType requestedType) {
		return (isOwner(requesterId) && requestedType.isShareable());
	}

	public boolean isOwner(String ownerId) {
		return owners.isEmpty() || owners.containsKey(ownerId);
	}

	private boolean isSingleOwner(String ownerId) {
		return owners.isEmpty() || (owners.size() == 1 && owners.containsKey(ownerId));
	}

	private boolean canUpdateOwnership(String ownerId) {
		return type.isShareable() && isOwner(ownerId);
	}

	private boolean canGiveOwnership(String ownerId, LockType requestedType) {
		return requestedType.isExclusive() && users.isEmpty() && isSingleOwner(ownerId);
	}

	private boolean canUpdateSharedLock(String ownerId, LockType requestedType) {
		return canGiveOwnership(ownerId, requestedType) || canUpdateOwnership(ownerId);
	}

	private boolean canMutateLock(String ownerId) {
		return isSingleOwner(ownerId) && users.isEmpty();
	}

	public boolean updateOwnership(String ownerId, String requesterId, LockType requestType) {
		Log.info(String.format("ownerId=%s requesterId=%s, requestType=%s, this=%s, canMudate=%s, canUpdate=%s",
				ownerId, requesterId, requestType, this, canMutateLock(ownerId), canUpdateOwnership(ownerId)));

		if (canMutateLock(ownerId)) {
			SharedLockUpdate op = new SharedLockUpdate(ownerId, requesterId, requestType, nextTimestamp(),
					requestType.isExclusive());
			applySharedLockUpdate(op);
			registerLocalOperation(op);
			return true;
		}

		if (canUpdateOwnership(ownerId)) {
			boolean isTransfer = requestType.isExclusive() && users.isEmpty();
			SharedLockUpdate op = new SharedLockUpdate(ownerId, requesterId, this.type, nextTimestamp(), isTransfer);
			applySharedLockUpdate(op);
			registerLocalOperation(op);
			Log.info("SHARED " + requestType + " WITH: " + requesterId + " NOW:" + this);
			return true;
		}
		return false;
	}

	public boolean lock(String ownerId, LockType request, Timestamp ts) {

		if (owners.isEmpty())
			updateOwnership(ownerId, ownerId, request);

		if (isOwner(ownerId)) {

			if (type.equals(request) && (type.isShareable() || users.isEmpty())) {
				users.add(ts);
				return true;
			}
			if (canMutateLock(ownerId)) {
				type = request;
				users.add(ts);
				return true;
			}
		}
		return false;
	}

	public boolean unlock(Timestamp ts) {
		return users.remove(ts);
	}

	protected void applySharedLockUpdate(SharedLockUpdate op) {
		Log.info("SharedLockOwnershipUpdate: " + op);
		// Check if the pre-condition of updateOwnership is still valid
		if (!canUpdateSharedLock(op.ownerId(), op.getType()))
			throw new IncompatibleLockException("Can't get ownership on downstream! op: " + op + " current: " + this);

		if (op.isTransfer())
			owners.remove(op.ownerId());

		type = op.getType();

		Set<TripleTimestamp> tsRequester = owners.get(op.requesterId());
		if (tsRequester == null) {
			tsRequester = new HashSet<TripleTimestamp>();
			owners.put(op.requesterId(), tsRequester);
		}
		tsRequester.add(op.getTimestamp());
	}

	public void setTxnHandle(TxnHandle handle) {
		super.txn = handle;
	}

	public Set<String> owners() {
		return new HashSet<String>(owners.keySet());
	}

	public void setOutdated() {
		this.outdated = true;
	}

	public boolean isOutdated() {
		return outdated;
	}

	public String toString() {
		return String.format("type: %s, owners: %s, active: %s, outdated: %s", type, owners.keySet(), users, outdated);
	}
}
