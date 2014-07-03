package swift.dc;

import java.util.concurrent.ConcurrentHashMap;

import swift.proto.CommitTimestampRequest;
import swift.proto.CommitUpdatesRequest;
import swift.proto.GenerateTimestampRequest;
import sys.utils.FifoQueue;

final public class FifoQueues {

	private final ConcurrentHashMap<String, FifoQueue<CommitUpdatesRequest>> txn;
	private final ConcurrentHashMap<String, FifoQueue<GenerateTimestampRequest>> gts;
	private final ConcurrentHashMap<String, FifoQueue<CommitTimestampRequest>> cts;

	FifoQueues() {
		this.txn = new ConcurrentHashMap<>();
		this.gts = new ConcurrentHashMap<>();
		this.cts = new ConcurrentHashMap<>();
	}

	FifoQueue<GenerateTimestampRequest> queue4GenTS(final String clientId) {
		FifoQueue<GenerateTimestampRequest> res = gts.get(clientId), nq;
		if (res == null) {
			res = gts.putIfAbsent(clientId, nq = new FifoQueue<GenerateTimestampRequest>(clientId));
			if (res == null)
				res = nq;
		}
		return res;
	}

	FifoQueue<CommitTimestampRequest> queue4CommitTS(final String clientId) {
		FifoQueue<CommitTimestampRequest> res = cts.get(clientId), nq;
		if (res == null) {
			res = cts.putIfAbsent(clientId, nq = new FifoQueue<CommitTimestampRequest>(clientId));
			if (res == null)
				res = nq;
		}
		return res;
	}

	FifoQueue<CommitUpdatesRequest> queue4CommitTxn(final String clientId) {
		FifoQueue<CommitUpdatesRequest> res = txn.get(clientId), nq;
		if (res == null) {
			res = txn.putIfAbsent(clientId, nq = new FifoQueue<CommitUpdatesRequest>(clientId));
			if (res == null)
				res = nq;
		}
		return res;
	}
}
