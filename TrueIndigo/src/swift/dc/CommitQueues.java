package swift.dc;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import swift.proto.CommitUpdatesRequest;
import sys.utils.TaskQueue;

final public class CommitQueues {

	private final ConcurrentHashMap<String, TaskQueue<CommitUpdatesRequest>> txn;

	CommitQueues() {
		this.txn = new ConcurrentHashMap<>();
	}

	TaskQueue<CommitUpdatesRequest> queue4CommitTxn(final String id) {
		TaskQueue<CommitUpdatesRequest> res = txn.get(id), nq;
		if (res == null) {
			res = txn.putIfAbsent(id, nq = new TaskQueue<CommitUpdatesRequest>(id));
			if (res == null)
				res = nq;
		}
		return res;
	}

	Collection<TaskQueue<CommitUpdatesRequest>> all() {
		return txn.values();
	}
}
