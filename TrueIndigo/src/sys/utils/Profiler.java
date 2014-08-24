package sys.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.utils.Pair;

public class Profiler {

	protected static String DEFAULT_FIELD_SEPARATOR = "\t";

	private static Map<Long, Pair<String, OperationStats>> ops;

	private static Map<String, Logger> loggers;

	private static AtomicLong opIdGenerator;

	private static Profiler instance;

	private Profiler() {
		ops = new HashMap<Long, Pair<String, OperationStats>>();
		opIdGenerator = new AtomicLong();
		loggers = new HashMap<>();
	}

	public long startOp(String loggerName, String operationName) {
		long opId = opIdGenerator.getAndIncrement();
		ops.put(opId, new Pair<>(loggerName, new OperationStats(operationName, System.currentTimeMillis())));
		return opId;
	}

	public void endOp(long opId, String... otherFields) {
		Pair<String, OperationStats> op = ops.get(opId);
		op.getSecond().endOperation(System.currentTimeMillis(), otherFields);
		Logger logger = getLogger(op.getFirst());
		if (logger.isLoggable(Level.FINEST)) {
			logger.finest(op.getSecond().toString());
		}
	}

	public static Profiler getInstance() {
		if (instance == null) {
			instance = new Profiler();
		}
		return instance;
	}

	public void printHeaderWithCustomFields(String loggerName, String... optionalFields) {
		String headerString = "OP_NAME" + DEFAULT_FIELD_SEPARATOR + "START_TIME" + DEFAULT_FIELD_SEPARATOR + "END_TIME"
				+ DEFAULT_FIELD_SEPARATOR + "DURATION";

		if (optionalFields.length > 0) {
			for (String field : optionalFields) {
				headerString += DEFAULT_FIELD_SEPARATOR;
				headerString += field;
			}
		}
		Logger logger = getLogger(loggerName);
		if (logger.isLoggable(Level.FINEST)) {
			logger.finest(headerString);
		}
	}
	private Logger getLogger(String loggerName) {
		if (!loggers.containsKey(loggerName)) {
			loggers.put(loggerName, Logger.getLogger(loggerName));
		}
		return loggers.get(loggerName);
	}
}

class OperationStats {
	private long startTimeMillis;
	private long endTimeMillis;
	private String operationName;
	private String[] otherFields;

	public OperationStats(String operationName, long startTimeMillis) {
		super();
		this.startTimeMillis = startTimeMillis;
		this.operationName = operationName;
	}

	public void endOperation(long endTimeMillis, String... otherFields) {
		this.endTimeMillis = endTimeMillis;
		this.otherFields = otherFields;
	}

	public String toString() {
		StringBuilder string = new StringBuilder();
		string.append(operationName);
		string.append(Profiler.DEFAULT_FIELD_SEPARATOR);
		string.append(startTimeMillis);
		string.append(Profiler.DEFAULT_FIELD_SEPARATOR);
		string.append(endTimeMillis);
		string.append(Profiler.DEFAULT_FIELD_SEPARATOR);
		string.append(endTimeMillis - startTimeMillis);
		if (otherFields != null && otherFields.length > 0) {
			string.append(Profiler.DEFAULT_FIELD_SEPARATOR);
			string.append(otherFields[0]);
			for (int i = 1; i < otherFields.length; i++) {
				string.append(Profiler.DEFAULT_FIELD_SEPARATOR);
				string.append(otherFields[i]);
			}
		}
		return string.toString();
	}

}
