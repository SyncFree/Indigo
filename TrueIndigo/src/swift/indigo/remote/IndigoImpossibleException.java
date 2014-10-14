package swift.indigo.remote;

import java.util.Collection;

import swift.api.CRDTIdentifier;
import swift.exceptions.SwiftException;

public class IndigoImpossibleException extends SwiftException {
	private Collection<CRDTIdentifier> resources;

	public IndigoImpossibleException(Collection<CRDTIdentifier> resources) {
		super("IMPOSSIBLE TO ACQUIRE RESOURCES " + resources);
		this.resources = resources;
	}

	public IndigoImpossibleException(Exception exception) {
		super(exception);
	}

	public Collection<CRDTIdentifier> getResources() {
		return resources;
	}

	private static final long serialVersionUID = 1L;

}
