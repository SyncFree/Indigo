/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
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
package sys.pubsub.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import sys.pubsub.PubSub;
import sys.pubsub.PubSub.Notifyable;
import sys.pubsub.PubSub.Publisher;
import sys.pubsub.PubSub.Subscriber;

public class AbstractPubSub<T> implements PubSub<T>, Subscriber<T>, Publisher<T, Notifyable<T>> {

	protected final String id;
	protected final Map<T, Set<Subscriber<T>>> subscribers;

	protected AbstractPubSub(String id) {
		this.id = id;
		this.subscribers = new ConcurrentHashMap<T, Set<Subscriber<T>>>();
	}

	@Override
	public String id() {
		return id;
	}

	@Override
	public void publish(Notifyable<T> info) {
		info.notifyTo(this);
	}

	@Override
	public boolean subscribe(T key, Subscriber<T> subscriber) {
		Set<Subscriber<T>> res = subscribers.get(key), nset;
		if (res == null) {
			res = subscribers.put(key, nset = new CopyOnWriteArraySet<Subscriber<T>>());
			if (res == null)
				res = nset;
		}
		return res.add(subscriber);
	}

	@Override
	public boolean unsubscribe(T key, Subscriber<T> subscriber) {
		Set<Subscriber<T>> ss = subscribers.get(key);
		return ss != null && ss.remove(subscriber) && ss.isEmpty();
	}

	@Override
	public boolean subscribe(Set<T> keys, Subscriber<T> Subscriber) {
		boolean changed = false;
		for (T i : keys)
			changed |= subscribe(i, Subscriber);

		return changed;
	}

	@Override
	public boolean unsubscribe(Set<T> keys, Subscriber<T> subscriber) {
		boolean changed = false;
		for (T i : keys)
			changed |= unsubscribe(i, subscriber);
		return changed;
	}

	@Override
	public Set<Subscriber<T>> subscribers(T key) {
		Set<Subscriber<T>> res = subscribers.get(key);
		return res != null ? res : Collections.unmodifiableSet(Collections.<Subscriber<T>> emptySet());
	}

	@Override
	public synchronized Set<Subscriber<T>> subscribers(Set<T> keys) {
		Set<Subscriber<T>> res = new HashSet<Subscriber<T>>();
		for (T i : keys)
			res.addAll(subscribers(i));
		return res;
	}

	@Override
	public boolean isSubscribed(T key, Subscriber<T> handler) {
		return subscribers(key).contains(handler);
	}

	@Override
	public boolean isSubscribed(T key) {
		return subscribers.containsKey(key);
	}

}
