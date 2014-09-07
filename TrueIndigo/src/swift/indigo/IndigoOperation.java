package swift.indigo;

import swift.proto.ClientRequest;

public abstract class IndigoOperation extends ClientRequest implements Comparable<IndigoOperation> {

	protected String requesterId;

	public IndigoOperation() {
		super();
	}

	public IndigoOperation(String requesterId) {
		super(requesterId);
	}

	public abstract void deliverTo(ResourceManagerNode node);

}
