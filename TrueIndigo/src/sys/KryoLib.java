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
package sys;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import sys.net.impl.GID;
import sys.net.impl.ServicePacket;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;

public class KryoLib {
	private static Logger Log = Logger.getLogger(KryoLib.class.getName());

	synchronized public static <T> T copy(T obj) {
		return kryo().copy(obj);
	}

	synchronized public static <T> T copyShallow(T obj) {
		return kryo().copyShallow(obj);
	}

	public static Kryo getKryoInstance() {
		Kryo res = new Kryo();

		for (_Registration i : registry)
			if (i.ser != null)
				res.register(i.cl, i.ser, i.id);
			else
				res.register(i.cl, i.id);

		// res.setInstantiatorStrategy(new StdInstantiatorStrategy());
		res.setAsmEnabled(true);
		res.setReferences(false);
		res.setAutoReset(true);
		return res;
	}

	synchronized public static void register(Class<?> cl, int id) {
		for (_Registration i : registry)
			if (i.id == id) {
				Log.severe("Already Registered..." + id);
				Thread.dumpStack();
			}
		registry.add(new _Registration(cl, null, id));
	}

	synchronized public static <T> void register(Class<T> cl, Serializer<? super T> serializer, int id) {
		for (_Registration i : registry)
			if (i.id == id) {
				Log.severe("Already Registered..." + id);
				Thread.dumpStack();
			}
		registry.add(new _Registration(cl, serializer, id));
	}

	synchronized public static void init() {

		// register(LocalEndpoint.class, new Serializer<AbstractEndpoint>() {
		//
		// @Override
		// final public AbstractEndpoint read(Kryo kryo, Input input,
		// Class<AbstractEndpoint> arg2) {
		// return new RemoteEndpoint(input.readLong(), input.readLong());
		// }
		//
		// @Override
		// final public void write(Kryo kryo, Output output, AbstractEndpoint
		// val) {
		// output.writeLong(val.locator);
		// output.writeLong(val.gid);
		// }
		//
		// }, 0x20);
		// register(RemoteEndpoint.class, new Serializer<AbstractEndpoint>() {
		//
		// @Override
		// final public AbstractEndpoint read(Kryo kryo, Input input,
		// Class<AbstractEndpoint> arg2) {
		// return new RemoteEndpoint(input.readLong(), input.readLong());
		// }
		//
		// @Override
		// final public void write(Kryo kryo, Output output, AbstractEndpoint
		// val) {
		// output.writeLong(val.locator);
		// output.writeLong(val.gid);
		// }
		//
		// }, 0x21);
		//
		// register(RpcPacket.class, 0x22);
		// register(InitiatorInfo.class, 0x23);
		// register(DHT_Request.class, 0x24);
		// register(DHT_RequestReply.class, 0x25);
		// register(StringKey.class, 0x26);

		register(GID.class, 0x20);
		register(ServicePacket.class, 0x21);
	}

	static class _Registration implements Comparable<_Registration> {

		int id;
		Class<?> cl;
		Serializer<?> ser;

		_Registration(Class<?> cl, Serializer<?> ser, int id) {
			this.cl = cl;
			this.ser = ser;
			this.id = id;
		}

		public String toString() {
			return String.format("<%s, %d>", cl.getSimpleName(), id);
		}

		public int hashCode() {
			return id;
		}

		public boolean equals(Object other) {
			return other != null && ((_Registration) other).id == id;
		}

		@Override
		public int compareTo(_Registration other) {
			return id - other.id;
		}
	}

	private static Set<_Registration> registry = new HashSet<_Registration>();

	// static class _Kryo extends Kryo {
	// SortedSet<_Registration> _registrations;
	//
	// _Kryo(Set<_Registration> rs) {
	// _registrations = new TreeSet<_Registration>(rs);
	// }
	//
	// public String toString() {
	// return _registrations.toString();
	// }
	// }

	private static final ThreadLocal<Kryo> kryo = new ThreadLocal<Kryo>() {
		@Override
		protected Kryo initialValue() {
			return getKryoInstance();
		}
	};

	public static Kryo kryo() {
		return kryo.get();
	}
}