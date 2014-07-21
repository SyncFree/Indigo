package swift.crdt;

import swift.api.CRDTIdentifier;

public class NonNegativeBoundedCounterAsResource extends BoundedCounterAsResource {

    public NonNegativeBoundedCounterAsResource() {
        super(new LowerBoundCounterCRDT());
    }

    public NonNegativeBoundedCounterAsResource(CRDTIdentifier identifier) {
        super(new LowerBoundCounterCRDT(identifier));
    }

}
