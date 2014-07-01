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
package sys.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.Channel;
import java.util.Collection;

public class IO {

	protected IO() {
	};

	public static void close(Socket s) {
		try {
			if (s != null)
				s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void close(ServerSocket s) {
		try {
			s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void close(InputStream in) {
		try {
			if (in != null)
				in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void close(OutputStream out) {
		try {
			if (out != null)
				out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void close(Channel ch) {
		try {
			if (ch != null)
				ch.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void redirect(String stdout, String stderr) {
		try {
			System.setOut(new PrintStream(new FileOutputStream(stdout)));
			System.setErr(new PrintStream(new FileOutputStream(stderr)));
		} catch (IOException x) {
			x.printStackTrace();
		}
	}

	public static void dumpTo(Collection<?> data, String dstFile) {
		try {
			PrintStream ps = new PrintStream(dstFile);
			for (Object i : data)
				ps.println(i);
			ps.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
