package swift.crdt;

import swift.api.CRDTIdentifier;

public class NonNegativeBoundedCounterAsResource extends BoundedCounterAsResource {

	public NonNegativeBoundedCounterAsResource() {
		super();
	}

	public NonNegativeBoundedCounterAsResource(CRDTIdentifier identifier) {
		super(identifier, new LowerBoundCounterCRDT(identifier));
	}

}
