package sys.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Tasks {

	static private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

	static public void exec(double due, Runnable task) {
		scheduler.schedule(task, (int) (due * 1000), TimeUnit.MILLISECONDS);
	}

	static public void every(double period, Runnable task) {
		scheduler.scheduleAtFixedRate(task, 0L, (long) (period * 1000), TimeUnit.MILLISECONDS);
	}

	static public void every(double delay, double period, Runnable task) {
		scheduler.scheduleAtFixedRate(task, (long) (delay * 1000), (long) (period * 1000), TimeUnit.MILLISECONDS);
	}

	static public void every(double period, Callable<Boolean> task) {
		new _PeriodicTask(0.0, period, task);
	}

	static public void every(double delay, double period, Callable<Boolean> task) {
		new _PeriodicTask(delay, period, task);
	}

	static class _PeriodicTask implements Runnable {

		final ScheduledFuture<?> fut;
		final Callable<Boolean> closure;
		_PeriodicTask(double delay, double period, Callable<Boolean> c) {
			this.closure = c;
			fut = scheduler.scheduleAtFixedRate(this, (long) (delay * 1000), (long) (period * 1000), TimeUnit.MILLISECONDS);
		}

		@Override
		public void run() {
			try {
				if (closure.call() == false)
					fut.cancel(false);
			} catch (Exception e) {
			}
		}

	}
}
