package sys.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A collection of convenience methods for dealing with threads.
 * 
 * @author smduarte (smd@fct.unl.pt)
 * 
 */
final public class Threading {

	protected Threading() {
	}

	static public Thread newThread(boolean daemon, Runnable r) {
		Thread res = new Thread(r);
		res.setDaemon(daemon);
		return res;
	}

	static public Thread newThread(Runnable r, boolean daemon) {
		Thread res = new Thread(r);
		res.setDaemon(daemon);
		return res;
	}

	static public Thread newThread(String name, boolean daemon, Runnable r) {
		Thread res = new Thread(r);
		res.setName(Thread.currentThread() + "." + name);
		res.setDaemon(daemon);
		return res;
	}

	static public Thread newThread(String name, Runnable r, boolean daemon) {
		Thread res = new Thread(r);
		res.setName(Thread.currentThread() + "." + name);
		res.setDaemon(daemon);
		return res;
	}

	static public void sleep(long ms) {
		try {
			if (ms > 0)
				Thread.sleep(ms);
		} catch (InterruptedException x) {
			x.printStackTrace();
		}
	}

	static public void sleep(long ms, int ns) {
		try {
			if (ms > 0 || ns > 0)
				Thread.sleep(ms, ns);
		} catch (InterruptedException x) {
			x.printStackTrace();
		}
	}

	static public void waitOn(Object o) {
		try {
			o.wait();
		} catch (InterruptedException x) {
			x.printStackTrace();
		}
	}

	static public void waitOn(Object o, long ms) {
		try {
			if (ms > 0)
				o.wait(ms);
		} catch (InterruptedException x) {
			x.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	static public <T> T poll(SynchronousQueue<?> queue, int timeout) {
		try {
			return (T) queue.poll(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}
		return null;
	}

	static public <T> void put(SynchronousQueue<T> queue, T val) {
		while (true)
			try {
				queue.put(val);
				return;
			} catch (InterruptedException e) {
			}
	}

	static public void notifyOn(Object o) {
		o.notify();
	}

	static public void notifyAllOn(Object o) {
		o.notifyAll();
	}

	static public void synchronizedWaitOn(Object o) {
		synchronized (o) {
			try {
				o.wait();
			} catch (InterruptedException x) {
				x.printStackTrace();
			}
		}
	}

	static public void synchronizedWaitOn(Object o, long ms) {
		synchronized (o) {
			try {
				o.wait(ms);
			} catch (InterruptedException x) {
				x.printStackTrace();
			}
		}
	}

	static public void synchronizedNotifyOn(Object o) {
		synchronized (o) {
			o.notify();
		}
	}

	static public void synchronizedNotifyAllOn(Object o) {
		synchronized (o) {
			o.notifyAll();
		}
	}

	synchronized public static void dumpAllThreadsTraces() {
		Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
		loop : for (StackTraceElement[] trace : traces.values()) {
			for (int j = 0; j < trace.length; j++)
				if (trace[j].getClassName().startsWith("swift")) {
					for (int k = j; k < trace.length; k++)
						System.err.print(">>" + trace[k] + " ");
					System.err.println();
					continue loop;
				}
		}

	}

	static public ThreadFactory factory(final String name) {
		return new ThreadFactory() {
			int counter = 0;
			String callerName = Thread.currentThread().getName();

			@Override
			public Thread newThread(Runnable target) {
				Thread t = new Thread(target, callerName + "." + name + "-" + counter++);
				t.setDaemon(true);
				return t;
			}
		};
	}

	static public void awaitTermination(ExecutorService pool, int seconds) {
		try {
			pool.shutdown();
			pool.awaitTermination(seconds, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void lock(Object id) {
		ReentrantLock lock = locks.get(id), newLock;
		if (lock == null) {
			lock = locks.putIfAbsent(id, newLock = new ReentrantLock(true));
			if (lock == null)
				lock = newLock;
		}
		lock.lock();
	}

	public static void unlock(Object id) {
		ReentrantLock lock = locks.get(id);
		if (lock == null)
			throw new RuntimeException("Unbalanced unlock for :" + id);

		lock.unlock();
	}

	static ConcurrentHashMap<Object, ReentrantLock> locks = new ConcurrentHashMap<>();
}
