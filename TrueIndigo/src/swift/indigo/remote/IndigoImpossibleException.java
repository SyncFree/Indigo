package swift.indigo.remote;

import swift.exceptions.SwiftException;

public class IndigoImpossibleException extends SwiftException {

	public IndigoImpossibleException() {
		super("IMPOSSIBLE TO ACQUIRE RESOURCES");
	}

	public IndigoImpossibleException(Exception exception) {
		super(exception);
	}

	private static final long serialVersionUID = 1L;

}
