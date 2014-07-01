package sys;

import sys.api.Sys;
import sys.net.api.Networking;
public class Context {

	String creator;
	public static Sys Sys;
	public static Networking Networking;

	Context() {
		creator = Thread.currentThread().getName();
	}

	static {

		KryoLib.init();

		Context.Sys = new sys.impl.Sys(new Context()) {
		};

		Context.Networking = new sys.net.impl.Networking();
	}
}
