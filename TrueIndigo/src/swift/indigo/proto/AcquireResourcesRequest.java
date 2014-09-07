package swift.indigo.proto;

import java.util.Collection;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import swift.clocks.Timestamp;
import swift.indigo.IndigoOperation;
import swift.indigo.ReservationsProtocolHandler;
import swift.indigo.ResourceManagerNode;
import swift.indigo.ResourceRequest;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class AcquireResourcesRequest extends IndigoOperation {

	Timestamp clientTs;
	SortedSet<ResourceRequest<?>> requests;
	private transient Envelope handler;

	public AcquireResourcesRequest() {

	}

	public AcquireResourcesRequest(String clientId, Timestamp cltTimestamp, Collection<ResourceRequest<?>> resources) {
		super(clientId);
		this.clientTs = cltTimestamp;
		this.requests = new TreeSet<ResourceRequest<?>>(resources);
	}

	public AcquireResourcesRequest(AcquireResourcesRequest other) {
		super(other.clientId);
		this.clientTs = other.clientTs;
		this.requests = new TreeSet<ResourceRequest<?>>(other.requests);
	}

	@Override
	public void deliverTo(Envelope handle, MessageHandler handler) {
		((ReservationsProtocolHandler) handler).onReceive(handle, this);
	}

	public Timestamp getClientTs() {
		return clientTs;
	}

	public Collection<ResourceRequest<?>> getResources() {
		return requests;
	}

	public int hashCode() {
		return requests.hashCode() ^ getClientId().hashCode();
	}

	// public boolean equals(AcquireResourcesRequest other) {
	// // FIXME: Only compares that the client timestamp is the same? -- Seems
	// // to be working
	// return getClientId().equals(other.getClientId())/*
	// * &&
	// * requests.equals(other
	// * .requests)
	// */;
	// }

	@Override
	public boolean equals(Object other) {
		if (other instanceof AcquireResourcesRequest) {
			return this.compareTo((AcquireResourcesRequest) other) == 0;
		} else
			return false;
	}

	// TODO: Must test more the combination of locks and counters
	@Override
	public int compareTo(IndigoOperation o) {
		if (o instanceof AcquireResourcesRequest) {
			AcquireResourcesRequest other = (AcquireResourcesRequest) o;
			Iterator<ResourceRequest<?>> it = requests.iterator();
			Iterator<ResourceRequest<?>> otherIt = other.requests.iterator();
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
					return clientTs.compareTo(other.clientTs);
				} else
					return clientComparison;
			} else
				return diff;
		} else
			return Integer.MAX_VALUE;
	}
	public String toString() {
		String resourcesAsString = requests.toString();
		return String.format("Acquire: %s, %s : %s)", clientTs, getClientId(), resourcesAsString);
	}

	@Override
	public void deliverTo(ResourceManagerNode node) {
		node.processWithReply(handler, this);
	}

	public void setHandler(Envelope conn) {
		this.handler = conn;

	}

}
