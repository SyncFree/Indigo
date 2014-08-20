package swift.indigo;

public interface IndigoOperation extends Comparable<IndigoOperation> {

	public void deliverTo(ResourceManagerNode node);

}
