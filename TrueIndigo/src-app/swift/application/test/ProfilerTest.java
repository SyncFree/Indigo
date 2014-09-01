package swift.application.test;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.junit.Test;

import sys.utils.Profiler;

public class ProfilerTest {

	@Test
	public void testLog() throws InterruptedException, SecurityException, IOException {
		String logname = "log";
		Logger logger = Logger.getLogger(logname);
		// logger.setLevel(Level.INFO);

		FileHandler fileTxt = new FileHandler("./log.log");
		// Formatter formatterTxt = new Formatter() {
		//
		// @Override
		// public String format(LogRecord record) {
		// return record.getMessage();
		// }
		//
		// };
		//
		// fileTxt.setFormatter(formatterTxt);
		logger.addHandler(fileTxt);

		Profiler profiler = Profiler.getInstance();
		long opId = profiler.startOp(logname, "OP");
		Thread.sleep(1000);
		profiler.endOp(logname, opId);
	}
}
