package swift.indigo;

import java.util.LinkedHashMap;
import java.util.Map;

import swift.clocks.CausalityClock;
import swift.clocks.ClockFactory;
import swift.clocks.Timestamp;

public class Snapshots {

	synchronized void register(Timestamp ts, CausalityClock cc) {
		snapshots.putIfAbsent(ts, cc);
	}

	synchronized CausalityClock oldest() {
		if (snapshots.isEmpty())
			return oldest;
		else {
			return oldest = snapshots.values().iterator().next();
		}
	}

	synchronized boolean free(Timestamp ts) {
		CausalityClock cc = snapshots.remove(ts);
		if (snapshots.isEmpty() && cc != null)
			oldest = cc;
		return cc != null;
	}

	private CausalityClock oldest = ClockFactory.newClock();
	private Map<Timestamp, CausalityClock> snapshots = new LinkedHashMap<>();
}
