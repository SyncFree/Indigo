package swift.utils;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

public class LogSiteFormatter extends Formatter {

	private String siteId;

	public LogSiteFormatter(String siteId) {
		this.siteId = siteId;
	}

	public String format(LogRecord record) {
		StringBuilder builder = new StringBuilder(1000);
		builder.append(siteId);
		builder.append(": ");
		builder.append(super.formatMessage(record));
		builder.append("\n");
		return builder.toString();
	}

	public String getHead(Handler h) {
		return super.getHead(h);
	}

	public String getTail(Handler h) {
		return super.getTail(h);
	}

}