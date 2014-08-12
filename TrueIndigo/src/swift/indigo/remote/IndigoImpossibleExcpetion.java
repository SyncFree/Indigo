package swift.indigo.remote;

import swift.exceptions.SwiftException;

public class IndigoImpossibleExcpetion extends SwiftException {

	public IndigoImpossibleExcpetion() {
		super("IMPOSSIBLE TO ACQUIRE RESOURCES");
	}

	public IndigoImpossibleExcpetion(Exception exception) {
		super(exception);
	}

	private static final long serialVersionUID = 1L;

}
