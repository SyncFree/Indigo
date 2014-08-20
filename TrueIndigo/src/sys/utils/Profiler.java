package sys.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import swift.utils.Pair;

public class Profiler {

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
		if (!loggers.containsKey(loggerName)) {
			loggers.put(loggerName, Logger.getLogger(loggerName));
		}
		ops.put(opId, new Pair<>(loggerName, new OperationStats(operationName, System.currentTimeMillis())));
		return opId;
	}

	public void endOp(long opId, String... otherFields) {
		Pair<String, OperationStats> op = ops.get(opId);
		op.getSecond().endOperation(System.currentTimeMillis(), otherFields);
		loggers.get(op.getFirst()).info(op.getSecond().toString());
	}

	public void printMessage(String loggerName, String message) {
		if (!loggers.containsKey(loggerName)) {
			loggers.put(loggerName, Logger.getLogger(loggerName));
		}
		loggers.get(loggerName).info(message);
	}

	public static Profiler getInstance() {
		if (instance == null) {
			instance = new Profiler();
		}
		return instance;
	}
}

class OperationStats {
	private long startTimeMillis;
	private long endTimeMillis;
	private String operationName;
	private String[] otherFields;

	private static String DEFAULT_FIELD_SEPARATOR = "\t";

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
		string.append(DEFAULT_FIELD_SEPARATOR);
		string.append(startTimeMillis);
		string.append(DEFAULT_FIELD_SEPARATOR);
		string.append(endTimeMillis);
		string.append(DEFAULT_FIELD_SEPARATOR);
		string.append(endTimeMillis - startTimeMillis);
		if (otherFields.length > 0) {
			string.append(DEFAULT_FIELD_SEPARATOR);
			string.append(otherFields[0]);
			for (int i = 1; i < otherFields.length; i++) {
				string.append(DEFAULT_FIELD_SEPARATOR);
				string.append(otherFields[i]);
			}
		}
		return string.toString();
	}

}
