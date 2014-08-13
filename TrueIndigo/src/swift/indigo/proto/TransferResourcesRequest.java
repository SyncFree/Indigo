package swift.indigo.proto;

import java.util.Collection;

import swift.clocks.Timestamp;
import swift.indigo.IndigoOperation;
import swift.indigo.ReservationsProtocolHandler;
import swift.indigo.ResourceManagerNode;
import swift.indigo.ResourceRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class TransferResourcesRequest extends AcquireResourcesRequest implements IndigoOperation {

	private String destinationId;
	private int transferSeqNumber;

	public TransferResourcesRequest() {

	}

	public TransferResourcesRequest(String requesterId, String destinationId, Timestamp cltTimestamp,
			Collection<ResourceRequest<?>> resources, int transferSeqNumber) {
		super(requesterId, cltTimestamp, resources);
		this.destinationId = destinationId;
		this.transferSeqNumber = transferSeqNumber;
	}

	public TransferResourcesRequest(String destinationId, int transferSeqNumber, AcquireResourcesRequest request) {
		super(request);
		this.destinationId = destinationId;
		this.transferSeqNumber = transferSeqNumber;
	}

	@Override
	public void deliverTo(Envelope handle, MessageHandler handler) {
		((ReservationsProtocolHandler) handler).onReceive(handle, this);

	}

	public String toString() {
		return String.format("Transfer request to: %s, TS:%s ----> %s", destinationId, super.clientTs,
				super.requests.toString());
	}

	public String getDestination() {
		return destinationId;
	}

	@Override
	public int hashCode() {
		return (clientId + "" + destinationId + "" + transferSeqNumber).hashCode();
	}

	@Override
	public void deliverTo(ResourceManagerNode node) {
		node.process(this);
	}
}
