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
package bench;

import sys.net.api.Envelope;
import sys.net.api.Message;
import sys.net.api.MessageHandler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * 
 * @author smd
 * 
 */
public class Request implements Message, KryoSerializable {

	public int val;
	public double timestamp;

	Request() {
	}

	public Request(int val) {
		this.val = val;
		this.timestamp = System.nanoTime();
	}
	public Request(int val, double ts) {
		this.val = val;
		this.timestamp = ts;
	}

	public String toString() {
		return "request: " + val;
	}

	@Override
	final public void read(Kryo kryo, Input input) {
		this.val = input.readInt();
		this.timestamp = input.readDouble();
	}

	@Override
	final public void write(Kryo kryo, Output output) {
		output.writeInt(this.val);
		output.writeDouble(this.timestamp);
	}

	@Override
	public void deliverTo(Envelope sender, MessageHandler handler) {
		((ProbeHandler) handler).onReceive(sender, this);
	}
}
