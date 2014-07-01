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
package sys.net.impl;

import java.util.concurrent.atomic.AtomicLong;

import sys.net.api.Endpoint;
import umontreal.iro.lecuyer.stat.Tally;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class NetworkingEndpoint implements Endpoint, KryoSerializable {

	protected String url;

	protected Tally rtt = new Tally("rtt");
	protected AtomicLong incomingBytesCounter = new AtomicLong(0);
	protected AtomicLong outgoingBytesCounter = new AtomicLong(0);

	protected NetworkingEndpoint() {
		this.incomingBytesCounter = new AtomicLong(0);
		this.outgoingBytesCounter = new AtomicLong(0);
	}

	protected NetworkingEndpoint(String url) {
		this();
		this.url = url;
	}

	protected NetworkingEndpoint(String url, String defaultUrl) {
		Url u = new Url(url), df = new Url(defaultUrl);
		this.url = new Url(u.getProtocol(), u.getHost(), u.getPort(df.getPort(-1)), u.gid()).toString();
	}

	@Override
	public int hashCode() {
		return url.hashCode();
	}

	final public boolean equals(NetworkingEndpoint other) {
		return url.equals(other.url);
	}

	@Override
	public boolean equals(Object other) {
		return other != null && equals((NetworkingEndpoint) other);
	}

	@Override
	public String toString() {
		return url;
	}

	@Override
	final public AtomicLong incomingBytesCounter() {
		if (incomingBytesCounter == null)
			return (incomingBytesCounter = new AtomicLong(0));
		return incomingBytesCounter;
	}

	final public AtomicLong outgoingBytesCounter() {
		if (outgoingBytesCounter == null)
			return (outgoingBytesCounter = new AtomicLong(0));
		return outgoingBytesCounter;
	}

	@Override
	final public void write(Kryo kryo, Output output) {
		output.writeString(url);
	}

	@Override
	final public void read(Kryo kryo, Input input) {
		url = input.readString();
	}

	final public Tally rtt() {
		return rtt;
	}

	@Override
	public String url() {
		return url;
	}

	@Override
	public String url(int newPort) {
		Url u = new Url(url());
		return String.format("%s://%s:%s", u.getProtocol(), u.getHost(), newPort);
	}

}
