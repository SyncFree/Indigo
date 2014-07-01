package swift.pubsub;

import java.util.Set;

import swift.api.CRDTIdentifier;
import swift.crdt.core.ObjectUpdatesInfo;
import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;

public class UpdateNotification implements Notifyable<CRDTIdentifier> {

	public String srcId;
	public ObjectUpdatesInfo info;

	public UpdateNotification() {
	}

	public UpdateNotification(String srcId, ObjectUpdatesInfo info) {
		this.info = info;
		this.srcId = srcId;
	}

	@Override
	public void notifyTo(PubSub<CRDTIdentifier> pubsub) {
		pubsub.subscribers(info.getId()).forEach(i -> {
			try {
				((SwiftSubscriber) i).onNotification(this);
			} catch (Exception x) {
				x.printStackTrace();
			}
		});
	}

	@Override
	public Object src() {
		return srcId;
	}

	@Override
	public CRDTIdentifier key() {
		return info.getId();
	}

	@Override
	public Set<CRDTIdentifier> keys() {
		return null;
	}
}
