package sys.impl;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import sys.Context;

public class Sys implements sys.api.Sys {

	Context ctx;
	Random rg = new Random();

	long T0 = System.currentTimeMillis();

	protected Sys(Context ctx) {
		this.ctx = ctx;
	}

	// public synchronized static Sys singleton() {
	// if (singleton == null)
	// singleton = new Sys() {
	// };
	// return singleton;
	// }

	static Sys singleton;

	@Override
	public double currentTime() {
		return (System.currentTimeMillis() - T0) * 0.001;
	}

	public long timeMillis() {
		return System.currentTimeMillis() - T0;
	}

	@Override
	public String context() {
		return ctx.toString();
	}

	@Override
	public Random random() {
		return rg;
	}

	AtomicLong uploadedBytes = new AtomicLong(1);
	AtomicLong downloadedBytes = new AtomicLong(1);

	@Override
	public AtomicLong uploadedBytes() {
		return uploadedBytes;
	}

	@Override
	public AtomicLong downloadedBytes() {
		return downloadedBytes;
	}

}