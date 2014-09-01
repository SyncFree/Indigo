package sys.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import swift.indigo.IndigoOperation;
import swift.indigo.proto.AcquireResourcesRequest;
import swift.utils.Pair;

public class Profiler {

	protected static String DEFAULT_FIELD_SEPARATOR = "\t";

	private static Map<Long, Pair<String, OperationStats>> ops;

	private static Map<IndigoOperation, Pair<String, OperationStats>> requests;

	private static Map<String, Logger> loggers;

	private static AtomicLong opIdGenerator;

	private static Profiler instance;

	private Profiler() {
		ops = new ConcurrentHashMap<Long, Pair<String, OperationStats>>();
		requests = new ConcurrentHashMap<>();
		opIdGenerator = new AtomicLong();
		loggers = new HashMap<>();
	}

	public long startOp(String loggerName, String operationName) {
		Logger logger = getLogger(loggerName);
		if (logger.isLoggable(Level.FINEST)) {
			long opId = opIdGenerator.getAndIncrement();
			ops.put(opId, new Pair<>(loggerName, new OperationStats(operationName, System.nanoTime())));
			return opId;
		}
		return -1;
	}

	public void endOp(String loggerName, long opId, String... otherFields) {
		Logger logger = getLogger(loggerName);
		if (logger.isLoggable(Level.FINEST)) {
			Pair<String, OperationStats> op = ops.get(opId);
			op.getSecond().endOperation(System.nanoTime(), otherFields);
			if (logger.isLoggable(Level.FINEST)) {
				logger.finest(op.getSecond().toString());
			}
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

	public void printMessage(String loggerName, String dumpArgs) {
		Logger logger = getLogger(loggerName);
		logger.finest(dumpArgs);
	}

	/**
	 * Experimental feature to track the time the operation takes from entering
	 * the system and being answered. Not being used right now, just for some
	 * quick experiments.
	 * 
	 * @param loggerName
	 * @param request
	 */

	public void trackRequest(String loggerName, IndigoOperation request) {
		Logger logger = getLogger(loggerName);
		if (logger.isLoggable(Level.FINEST)) {
			requests.put(request,
					new Pair<>(loggerName, new OperationStats(request.getClass().toString(), System.nanoTime())));
		}
	}

	public void finishRequest(String loggerName, AcquireResourcesRequest request) {
		Logger logger = getLogger(loggerName);
		if (logger.isLoggable(Level.FINEST)) {
			Pair<String, OperationStats> req = requests.get(request);
			req.getSecond().endOperation(System.nanoTime());
			if (logger.isLoggable(Level.FINEST)) {
				logger.finest(req.getSecond().toString());
			}
			requests.remove(request);
		}
	}
}

class OperationStats {
	private long startTimeNanos;
	private long endTimeNanos;
	private String operationName;
	private String[] otherFields;

	public OperationStats(String operationName, long startTimeNanos) {
		super();
		this.startTimeNanos = startTimeNanos;
		this.operationName = operationName;
	}

	public void endOperation(long endTimeNanos, String... otherFields) {
		this.endTimeNanos = endTimeNanos;
		this.otherFields = otherFields;
	}

	public String toString() {
		StringBuilder string = new StringBuilder();
		string.append(operationName);
		string.append(Profiler.DEFAULT_FIELD_SEPARATOR);
		string.append(startTimeNanos);
		string.append(Profiler.DEFAULT_FIELD_SEPARATOR);
		string.append(endTimeNanos);
		string.append(Profiler.DEFAULT_FIELD_SEPARATOR);
		string.append((endTimeNanos - startTimeNanos) / 1000000);
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
