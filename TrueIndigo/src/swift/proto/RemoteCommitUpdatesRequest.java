package swift.proto;

import java.util.ArrayList;
import java.util.List;

import swift.clocks.CausalityClock;
import swift.clocks.Timestamp;
import swift.crdt.core.CRDTObjectUpdatesGroup;
import sys.net.api.Envelope;
import sys.net.api.MessageHandler;

public class RemoteCommitUpdatesRequest extends ClientRequest {
	protected List<CRDTObjectUpdatesGroup<?>> objectUpdateGroups;
	protected CausalityClock dependencyClock;
	protected Timestamp cltTimestamp;

	protected Timestamp timestamp;
	transient Timestamp prvCltTimestamp;
	transient Envelope source;

	public RemoteCommitUpdatesRequest() {
	}

	public RemoteCommitUpdatesRequest(CommitUpdatesRequest req) {
		super(req.clientId);
		this.cltTimestamp = req.cltTimestamp;
		this.dependencyClock = req.dependencyClock;
		this.objectUpdateGroups = new ArrayList<CRDTObjectUpdatesGroup<?>>(req.objectUpdateGroups);
	}

	public void setSource(Envelope source) {
		this.source = source;
	}

	public Envelope getSource() {
		return this.source;
	}

	public boolean isReadOnly() {
		return objectUpdateGroups.size() == 0;
	}

	public CausalityClock getDependencyClock() {
		return dependencyClock;
	}

	/**
	 * Record the timestamp obtained for this commit from the sequencer
	 * 
	 * @param timestamp
	 *            from the sequencer
	 */
	public void setTimestamps(Timestamp timestamp, Timestamp prvCltTimestamp) {
		this.timestamp = timestamp;
		this.prvCltTimestamp = prvCltTimestamp;
	}

	/**
	 * @return timestamp obtained from the sequencer...
	 */
	public Timestamp getTimestamp() {
		return timestamp;
	}

	/**
	 * @return valid base timestamp for all updates in the request, previously
	 *         obtained using {@link GenerateTimestampRequest}; all individual
	 *         updates use TripleTimestamps with this base Timestamp
	 */
	public Timestamp getCltTimestamp() {
		return cltTimestamp;
	}

	public Timestamp getPrvCltTimestamp() {
		return prvCltTimestamp;
	}

	/**
	 * @return list of groups of object operations; there is at most one group
	 *         per object; note that all groups share the same base client
	 *         timestamp ( {@link #getClientTimestamp()}), timestamp mappings
	 *         and dependency clock.
	 * 
	 */
	public List<CRDTObjectUpdatesGroup<?>> getObjectUpdateGroups() {
		return objectUpdateGroups;
	}

	public void addTimestampsToDeps(List<Timestamp> tsLst) {
		if (tsLst != null) {
			for (Timestamp t : tsLst) {
				this.dependencyClock.record(t);
			}
		}
	}

	@Override
	public String toString() {
		return "CommitUpdatesRequest [objectUpdateGroups=" + objectUpdateGroups + ", dependencyClock=" + dependencyClock + ", cltTimestamp=" + cltTimestamp + "]";
	}

	public void dropInternalDependency() {
		dependencyClock.drop(clientId);
	}

	@Override
	public void deliverTo(Envelope src, MessageHandler handler) {
		((SurrogateProtocol) handler).onReceive(src, this);
	}
}
