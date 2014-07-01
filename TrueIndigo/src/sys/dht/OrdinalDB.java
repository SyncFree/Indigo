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
package sys.dht;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import sys.net.api.Endpoint;
/**
 * 
 * @author smd
 * 
 */
public class OrdinalDB {

	Node self;
	SortedMap<Long, Node> k2n = new TreeMap<Long, Node>();

	public OrdinalDB populate(String dc, List<String> urls, Endpoint endpoint) {
		k2n.clear();

		SortedMap<Long, Node> tmp = new TreeMap<Long, Node>();

		for (String i : urls) {
			Node n = new Node(dc, i);
			tmp.put(n.key, n);
		}

		self = new Node(dc, endpoint.url());
		tmp.put(self.key, self);

		int total = tmp.size();

		int order = 0;
		for (Node i : tmp.values()) {
			long ordKey = (Node.MAX_KEY / total) * order++;
			k2n.put(ordKey, i.clone(ordKey));
		}

		for (Node i : k2n.values())
			if (i.endpoint.equals(endpoint)) {
				self = i;
				break;
			}
		return this;
	}

	public Node self() {
		return self;
	}

	public Node resolveKey(long key) {
		for (Node i : k2n.tailMap(key).values())
			return i;

		for (Node i : k2n.headMap(key).values())
			return i;

		return self;
	}

	public Set<Long> nodeKeys() {
		return k2n.keySet();
	}

	public Collection<Node> nodes() {
		return k2n.values();
	}
}
