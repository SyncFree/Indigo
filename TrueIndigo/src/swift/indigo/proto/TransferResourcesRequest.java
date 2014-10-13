package swift.indigo.proto;

import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import swift.clocks.Timestamp;
import swift.indigo.IndigoOperation;
import swift.indigo.ResourceManagerNode;
import swift.indigo.ResourceRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class TransferResourcesRequest extends IndigoOperation {

	private String destinationId;
	private int transferSeqNumber;
	private SortedSet<ResourceRequest<?>> resources;

	public TransferResourcesRequest() {

	}

	public TransferResourcesRequest(String requesterId, String destinationId, Timestamp cltTimestamp, Collection<ResourceRequest<?>> resources, int transferSeqNumber) {
		super(requesterId);
		this.resources = new TreeSet<ResourceRequest<?>>(resources);
		this.requesterId = requesterId;
		this.destinationId = destinationId;
		this.transferSeqNumber = transferSeqNumber;
	}

	public TransferResourcesRequest(String requesterId, String destinationId, int transferSeqNumber, Collection<ResourceRequest<?>> resources) {
		super(requesterId);
		this.resources = (SortedSet<ResourceRequest<?>>) resources;
		this.destinationId = destinationId;
		this.transferSeqNumber = transferSeqNumber;
	}

	@Override
	public void deliverTo(Envelope handle, MessageHandler handler) {
		((ReservationsProtocolHandler) handler).onReceive(handle, this);

	}

	public String toString() {
		return String.format("Transfer: %s, ----> %s", destinationId, resources.toString());
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

	@Override
	public boolean equals(Object other) {
		if (other instanceof TransferResourcesRequest) {
			return this.compareTo((TransferResourcesRequest) other) == 0;
		} else
			return false;
	}

	@Override
	public int compareTo(IndigoOperation o) {
		if (o instanceof TransferResourcesRequest) {
			TransferResourcesRequest other = ((TransferResourcesRequest) o);
			Iterator<ResourceRequest<?>> it = resources.iterator();
			Iterator<ResourceRequest<?>> otherIt = other.resources.iterator();
			int diff = 0;
			while (diff == 0) {
				if (it.hasNext()) {
					ResourceRequest myElem = it.next();
					if (otherIt.hasNext()) {
						ResourceRequest otherElem = otherIt.next();
						diff = myElem.compareTo(otherElem);
					} else {
						return 1;
					}
				} else {
					// If the resources request list is smaller it is first in
					// the lexicographical order
					if (otherIt.hasNext())
						return -1;
					else {
						break;
					}
				}
			}
			if (diff == 0) {
				int clientComparison = getClientId().compareTo(other.getClientId());
				if (clientComparison == 0) {
					return destinationId.compareTo(other.destinationId);
				} else
					return clientComparison;
			} else
				return diff;
		} else if (o instanceof AcquireResourcesRequest)
			return -1;
		else
			return 1;
	}

	public Collection<ResourceRequest<?>> getResources() {
		return resources;
	}

	public String key() {
		StringBuilder sb = new StringBuilder().append(destinationId);
		getResources().forEach(i -> {
			sb.append(i.getResourceId());
		});
		return sb.toString();
	}
}
