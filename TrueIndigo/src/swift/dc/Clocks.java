package swift.dc;

import java.util.concurrent.ConcurrentHashMap;

import swift.clocks.CausalityClock;
import swift.clocks.CausalityClock.CMP_CLOCK;
import swift.clocks.ClockFactory;
import swift.clocks.IncrementalTimestampGenerator;
import swift.clocks.Timestamp;
import sys.utils.Args;

final public class Clocks {

	final String owner;
	final CausalityClock notUsed;
	final CausalityClock clientClock;
	final CausalityClock currentClock;
	final IncrementalTimestampGenerator clockGen;

	final CausalityClock pruneClock;

	public Clocks(String owner) {
		this.owner = owner;
		this.notUsed = ClockFactory.newClock();
		this.pruneClock = ClockFactory.newClock();
		this.clientClock = ClockFactory.newClock();
		this.currentClock = ClockFactory.newClock();
		this.clockGen = new IncrementalTimestampGenerator(Args.valueOf("-site", "X"));
	}

	CMP_CLOCK cmp(CausalityClock clk, CausalityClock that) {
		synchronized (clk) {
			return clk.compareTo(that);
		}
	}

	public boolean record(Timestamp ts, CausalityClock clk) {
		synchronized (clk) {
			return clk.record(ts);
		}
	}

	public boolean record(Timestamp ts, CausalityClock... clocks) {
		boolean recorded = false;
		for (CausalityClock clk : clocks) {
			synchronized (clk) {
				recorded |= clk.record(ts);
			}
		}
		return recorded;
	}

	public void recordAllUntil(Timestamp ts, CausalityClock clk) {
		synchronized (clk) {
			clk.recordAllUntil(ts);
		}
	}

	public Timestamp newTimestamp() {
		return this.clockGen.generateNew();
	}

	public Timestamp getLatest(CausalityClock clk, String identifier) {
		synchronized (clk) {
			return clk.getLatest(identifier);
		}
	}

	public CausalityClock clientClockCopy() {
		synchronized (clientClock) {
			return clientClock.clone();
		}
	}

	public CausalityClock currentClockCopy() {
		synchronized (currentClock) {
			return currentClock.clone();
		}
	}

	public void updateCurrentClock(CausalityClock thatClock) {
		// System.err.println(owner + " Before:" + currentClock + " thatClock:"
		// + thatClock);
		synchronized (currentClock) {
			this.currentClock.merge(thatClock);
		}
	}

	public void updateClock(CausalityClock thisClock, CausalityClock thatClock) {
		// System.err.println(owner + " Before:" + thisClock + " thatClock:" +
		// thatClock);
		synchronized (thisClock) {
			thisClock.merge(thatClock);
		}
	}

	public CausalityClock getClock(String name) {
		CausalityClock res = clocks.get(name), nres;
		if (res == null) {
			res = clocks.putIfAbsent(name, nres = ClockFactory.newClock());
			if (res == null)
				res = nres;
		}
		return res;
	}

	ConcurrentHashMap<String, CausalityClock> clocks = new ConcurrentHashMap<String, CausalityClock>();
}
