package swift.crdt;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;

import swift.api.CRDTIdentifier;
import swift.api.TxnHandle;
import swift.clocks.CausalityClock;
import swift.crdt.core.BaseCRDT;
import swift.utils.Pair;

public abstract class BoundedCounterCRDT<T extends BoundedCounterCRDT<T>> extends BaseCRDT<T> {

	private static final Comparator<Pair<String, Integer>> DEFAUT_PREFERENCE_LIST = new Comparator<Pair<String, Integer>>() {

		@Override
		public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
			return o2.getSecond() - o1.getSecond();
		}
	};

	protected Map<String, Map<String, Integer>> permissions;
	protected Map<String, Integer> delta;
	protected int initVal, val;

	transient private boolean failed;
	transient private boolean invalidated;

	public BoundedCounterCRDT() {
		super();
	}

	public BoundedCounterCRDT(CRDTIdentifier id) {
		super(id);
	}

	public BoundedCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock) {
		super(id, txn, clock);
	}

	public BoundedCounterCRDT(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, int initVal,
			Map<String, Map<String, Integer>> permissions, Map<String, Integer> decrements) {
		super(id, txn, clock);
		this.permissions = permissions;
		this.delta = decrements;
		this.initVal = initVal;
		this.val = initVal + totalProduced() - totalSpent();
	}

	public BoundedCounterCRDT(CRDTIdentifier id, int initVal) {
		super(id);
		this.initVal = initVal;
		this.val = initVal;
		this.permissions = new HashMap<String, Map<String, Integer>>();
		this.delta = new HashMap<String, Integer>();

	}

	@Override
	public Integer getValue() {
		return val;
	}

	protected int totalSpent() {
		int totalSpent = 0;
		for (Integer spentSiteId : delta.values()) {
			totalSpent += spentSiteId;
		}
		return totalSpent;
	}

	protected int totalProduced() {
		int totalAvailable = 0;
		for (Entry<String, Map<String, Integer>> sitePermissions : permissions.entrySet()) {
			totalAvailable += sitePermissions.getValue().get(sitePermissions.getKey());
		}
		return totalAvailable;
	}

	protected void checkExistsPermissionPair(String leftId, String rightId) {
		if (!permissions.containsKey(leftId)) {
			HashMap<String, Integer> sitePerm = new HashMap<String, Integer>();
			sitePerm.put(rightId, 0);
			if (leftId.equals(rightId)) {
				delta.put(leftId, 0);
			}
			permissions.put(leftId, sitePerm);
		}

		if (!permissions.get(leftId).containsKey(rightId)) {
			permissions.get(leftId).put(rightId, 0);
		}
	}

	public int availableSiteId(String siteId) {
		int given = 0;
		int received = 0;
		int spent = 0;

		checkExistsPermissionPair(siteId, siteId);
		spent = delta.get(siteId);
		Map<String, Integer> sitePermissions = permissions.get(siteId);
		for (Entry<String, Integer> p : sitePermissions.entrySet()) {
			if (!p.getKey().equals(siteId)) {
				given += p.getValue();
			}
		}
		for (Entry<String, Map<String, Integer>> allPermissions : permissions.entrySet()) {
			Integer receivedPermissions = allPermissions.getValue().get(siteId);
			if (receivedPermissions != null) {
				received += receivedPermissions;
			}
		}
		return received - given - spent;
	}

	public int givenTo(String giver, String receiver) {
		Map<String, Integer> sitePermissions = permissions.get(giver);
		Integer val = sitePermissions.get(receiver);
		return val == null ? 0 : val;
	}

	public Queue<Pair<String, Integer>> preferenceList() {
		return preferenceList(null);
	}

	public Queue<Pair<String, Integer>> preferenceList(String excludeSiteId) {
		PriorityQueue<Pair<String, Integer>> preferenceList = new PriorityQueue<Pair<String, Integer>>(1,
				DEFAUT_PREFERENCE_LIST);
		for (String site : permissions.keySet()) {
			if (excludeSiteId != null && !excludeSiteId.equals(site))
				preferenceList.add(new Pair<String, Integer>(site, availableSiteId(site)));
		}
		return preferenceList;
	}

	public void applyTransfer(BoundedCounterTransfer<T> transferUpdate) {
		checkExistsPermissionPair(transferUpdate.getOriginId(), transferUpdate.getTargetId());
		Map<String, Integer> targetPermissions = permissions.get(transferUpdate.getOriginId());
		targetPermissions.put(transferUpdate.getTargetId(), targetPermissions.get(transferUpdate.getTargetId())
				+ transferUpdate.getAmount());
	}

	public abstract boolean decrement(int amount, String siteId);

	public abstract boolean increment(int amount, String siteId);

	public abstract boolean transfer(int amount, String originId, String targetId);

	protected abstract void applyInc(BoundedCounterIncrement<T> incUpdate);

	protected abstract void applyDec(BoundedCounterDecrement<T> decUpdate);

	public boolean isInvalidated() {
		return invalidated;
	}

	public void invalidate() {
		invalidated = true;
	}

	public boolean isFailed() {
		return failed;
	}

	public void setFailed() {
		failed = true;
	}

	// TODO: Not implemented - but should not be necessary!
	public boolean isSingleOwner(String siteId) {
		return false;
	}

}
