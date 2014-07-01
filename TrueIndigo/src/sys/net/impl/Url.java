package sys.net.impl;

import sys.utils.IP;
import sys.utils.Numbers;

public class Url {

	private int port = -1;
	private String gid;
	private String host;
	private String protocol;

	public Url(String url) {
		parseUrl(url);
	}

	public Url(String protocol, String host, int port, String gid) {
		this.protocol = protocol;
		this.host = host;
		this.port = port;
		this.gid = gid;
	}

	public String getProtocol() {
		return protocol;
	}

	public String getHost() {
		return host;
	}

	public int getPort(int defaultPort) {
		return port < 0 ? defaultPort : port;
	}

	public String gid() {
		return gid;
	}

	public String siteId() {
		return gid == null ? "*" : gid;
	}

	private void parseUrl(String url) {
		String _protocol = "tcp", _host = "*", _port = "-1", _gid = null;
		try {
			boolean hasProto = url.contains("://");
			String[] tok = url.split("[/:]");
			// System.err.println(hasProto + "  " + Arrays.asList(tok) + "  " +
			// tok.length);
			switch (tok.length) {
				case 1 :
					if (Numbers.isInteger(tok[0].trim()))
						_port = tok[0];
					else
						_host = tok[0];

					break;
				case 2 :
					if (Numbers.isInteger(tok[0])) {
						_port = tok[0];
						_gid = tok[1];
					} else {
						if (Numbers.isInteger(tok[1])) {
							_host = tok[0];
							_port = tok[1];
						} else {
							_host = tok[0];
							_gid = tok[1];
						}
					}
					break;
				case 3 :
					_host = tok[0];
					_port = tok[1];
					_gid = tok[2];
					break;
				case 5 :
					if (hasProto) {
						_protocol = tok[0];
						_host = tok[3];
						_port = tok[4];
					} else {
						_host = tok[0];
						_port = tok[1];
						_gid = tok[2];
					}
					break;
				case 6 :
				default :
					_protocol = tok[0];
					_host = tok[3];
					_port = tok[4];
					_gid = tok[5].trim();
			}
		} catch (Exception x) {
			System.err.println("Malformed url: " + url);
			x.printStackTrace();
		}
		this.protocol = _protocol.trim();
		this.host = _host.trim().equals("*") ? IP.localHostAddressString() : _host.trim();
		this.port = Integer.parseInt(_port.trim());
		this.gid = _gid;
	}
	public String toString() {
		return String.format("%s://%s:%s%s", protocol, host, (port < 0) ? "" : "" + port, gid == null ? "" : "/" + gid);
	}

	public static void main(String[] args) {
		Url x = new Url("tcp://*:3333/375637456375");
		System.err.println(x);
		Url y = new Url("3333");
		System.err.println(y + " / " + y.siteId());
		Url z = new Url("193.136.1.1:3333/Z");
		System.err.println(z);
		Url w = new Url("3333/Z");
		System.err.println(w.siteId());
	}
}
