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
package sys.dht;

import static sys.Context.Networking;
import sys.net.api.Endpoint;
/**
 * 
 * @author smduarte
 * 
 */
public class Node {

	static final int NODE_KEY_LENGTH = 10;
	public static final long MAX_KEY = (1L << NODE_KEY_LENGTH) - 1L;

	public long key;
	public String datacenter;
	public Endpoint endpoint;
	public Endpoint dhtEndpoint;

	public Node() {
	}

	protected Node(Node other) {
		this.key = other.key;
		this.endpoint = other.endpoint;
		this.dhtEndpoint = other.dhtEndpoint;
		this.datacenter = other.datacenter;
	}

	public Node(String datacenter, String url) {
		this.datacenter = datacenter;
		this.endpoint = Networking.resolve(url);
		this.key = url2key(endpoint.url());
	}

	Node clone(long newKey) {
		Node res = new Node();
		res.key = newKey;
		res.endpoint = this.endpoint;
		res.datacenter = this.datacenter;
		res.dhtEndpoint = this.dhtEndpoint;
		return res;
	}

	@Override
	public int hashCode() {
		return (int) ((key >>> 32) ^ key);
	}

	public boolean equals(Object other) {
		return other != null && ((Node) other).key == key;
	}

	public String getDatacenter() {
		return datacenter;
	}

	@Override
	public String toString() {
		return "" + key + " -> " + endpoint;
	}

	private static long url2key(Object locator) {
		return (DHT_Node.longHashValue(locator.toString()) >>> 1) & MAX_KEY;
	}
}