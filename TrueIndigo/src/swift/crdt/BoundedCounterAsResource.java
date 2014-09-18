package swift.crdt;

import java.util.Collection;
import java.util.Queue;

import swift.api.CRDTIdentifier;
import swift.api.TxnHandle;
import swift.clocks.CausalityClock;
import swift.indigo.ConsumableResource;
import swift.indigo.CounterReservation;
import swift.indigo.ResourceRequest;
import swift.indigo.TRANSFER_STATUS;
import swift.utils.Pair;

public class BoundedCounterAsResource extends BoundedCounterCRDT<BoundedCounterAsResource> implements ConsumableResource<Integer> {

	private BoundedCounterCRDT<LowerBoundCounterCRDT> counter;

	public BoundedCounterAsResource() {
		super();
	}

	public BoundedCounterAsResource(CRDTIdentifier identifier) {
		super(identifier);
		this.counter = new LowerBoundCounterCRDT(identifier);
	}

	public BoundedCounterAsResource(CRDTIdentifier identifier, BoundedCounterCRDT<LowerBoundCounterCRDT> counter) {
		super(identifier);
		this.counter = counter;
	}

	@Override
	public void initialize(String ownerId, ResourceRequest<Integer> request) {
		this.counter.increment((int) request.getResource(), ownerId);
	}

	@Override
	public void produce(String ownerId, Integer req) {
		counter.increment(req, ownerId);
	}

	@Override
	public boolean consume(String ownerId, ResourceRequest<Integer> req) {
		return counter.decrement(req.getResource(), ownerId);
	}

	@Override
	public TRANSFER_STATUS transferOwnership(String fromId, String toId, ResourceRequest<Integer> request) {
		return (counter.transfer(request.getResource(), fromId, toId)) ? TRANSFER_STATUS.SUCCESS : TRANSFER_STATUS.FAIL;
	}

	@Override
	public boolean checkRequest(String ownerId, ResourceRequest<Integer> request) {
		return counter.availableSiteId(ownerId) >= request.getResource();
	}

	@Override
	public boolean isOwner(String siteId) {
		return counter.availableSiteId(siteId) > 0;
	}

	@Override
	public Integer getSiteResource(String siteId) {
		return counter.availableSiteId(siteId);
	}

	@Override
	public Integer getCurrentResource() {
		return counter.totalProduced() - counter.totalSpent();
	}

	@Override
	public boolean isReservable() {
		return getCurrentResource() != 0;
	}

	@Override
	public Integer getValue() {
		return counter.getValue();
	}

	@Override
	public CRDTIdentifier getUID() {
		return counter.getUID();
	}

	@Override
	public TxnHandle getTxnHandle() {
		return counter.getTxnHandle();
	}

	@Override
	public CausalityClock getClock() {
		return counter.getClock();
	}

	@Override
	public boolean decrement(int amount, String siteId) {
		return consume(siteId, new CounterReservation(siteId, getUID(), amount));
	}

	@Override
	public boolean increment(int amount, String siteId) {
		produce(siteId, amount);
		return true;
	}

	@Override
	public boolean transfer(int amount, String originId, String targetId) {
		return counter.transfer(amount, originId, targetId);
	}

	@Override
	public BoundedCounterAsResource copy() {
		return new BoundedCounterAsResource(id, counter.copy());
	}

	@Override
	protected void applyInc(BoundedCounterIncrement<BoundedCounterAsResource> incUpdate) {
		counter.applyInc((BoundedCounterIncrement) incUpdate);
	}

	@Override
	protected void applyDec(BoundedCounterDecrement<BoundedCounterAsResource> decUpdate) {
		counter.applyDec((BoundedCounterDecrement) decUpdate);
	}

	@Override
	protected void applyTransfer(BoundedCounterTransfer<BoundedCounterAsResource> transferUpdate) {
		counter.applyTransfer((BoundedCounterTransfer) transferUpdate);
	}

	@Override
	public BoundedCounterAsResource copyWith(TxnHandle txnHandle, CausalityClock clock) {
		BoundedCounterAsResource copy = super.copyWith(txnHandle, clock);
		copy.counter = counter.copyWith(txnHandle, clock);
		return copy;
	}

	@Override
	public void apply(String siteId, ResourceRequest<Integer> req) {
		if (req instanceof BoundedCounterIncrement) {
			counter.applyInc((BoundedCounterIncrement) req);
		} else if (req instanceof BoundedCounterDecrement) {
			counter.applyDec((BoundedCounterDecrement) req);
		} else if (req instanceof CounterReservation) {
			counter.applyDec(new BoundedCounterDecrement<>(req.getRequesterId(), req.getResource()));
		}
	}

	@Override
	public Collection<String> getAllResourceOwners() {
		return counter.permissions.keySet();
	}

	@Override
	public boolean isSingleOwner(String siteId) {
		return counter.isSingleOwner(siteId);
	}

	// TODO: Always return false - other strategy could be used
	@Override
	public boolean releaseShare(String ownerId) {
		return false;
	}

	@Override
	public String toString() {
		return counter.toString();
	}

	public Queue<Pair<String, Integer>> preferenceList() {
		return counter.preferenceList();
	}

	public Queue<Pair<String, Integer>> preferenceList(String excludeSiteId) {
		return counter.preferenceList(excludeSiteId);
	}

}
