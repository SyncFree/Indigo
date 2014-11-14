/*****************************************************************************
 * Copyright 2011-2014 INRIA
 * Copyright 2011-2014 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
package sys.utils;

import java.util.concurrent.ConcurrentLinkedDeque;

public class TaskQueue<T> {

	private static final long RETRY_PERIOD = 50;
	protected String name;

	protected ConcurrentLinkedDeque<Task<T>> queue;

	public interface TaskHandler<T> {
		public boolean execute(final T task);
	}

	public TaskQueue() {
		this("?");
	}

	public TaskQueue(String name) {
		this.name = name;
		this.queue = new ConcurrentLinkedDeque<>();

		Threading.newThread(true, () -> {
			for (;;) {
				Task<T> task;
				synchronized (queue) {
					while (queue.isEmpty())
						Threading.waitOn(queue, RETRY_PERIOD);

					task = queue.peek();
				}

				for (;;) {
					synchronized (this) {
						try {
							if (task.handler.execute(task.task)) {
								queue.removeFirst();
								break;
							}
						} catch (Exception x) {
							x.printStackTrace();
						}
						Threading.waitOn(this, RETRY_PERIOD);
					}
				}
			}
		}).start();
	}

	public void offer(T task, TaskHandler<T> handler) {
		synchronized (queue) {
			queue.offer(new Task<>(task, handler));
			Threading.notifyAllOn(queue);
		}
	}

	public void retry() {
		Threading.synchronizedNotifyAllOn(this);
	}

	protected static class Task<T> {
		final T task;
		final TaskHandler<T> handler;

		Task(T task, TaskHandler<T> handler) {
			this.task = task;
			this.handler = handler;
		}
	}
}