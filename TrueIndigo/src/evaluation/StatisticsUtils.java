package evaluation;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.math3.stat.Frequency;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class StatisticsUtils {
	private static final long ONE_SECOND_IN_NANOS = 1000000000;
	private static final long ONE_MINUTE_IN_NANOS = ONE_SECOND_IN_NANOS * 60;
	private static final long WARMUP_TIME = ONE_SECOND_IN_NANOS * 30;
	private static final int OUTLIER = 3000;
	static Frequency valuesFreq;

	public static void getCDF(int rangeI, int rangeF, int increment) {
		long startTime = -1;
		valuesFreq = new Frequency();
		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNext()) {
			if (scanner.hasNextLong()) {
				long opStartTime = scanner.nextLong();
				long opTime = scanner.nextLong();
				if (startTime == -1) {
					startTime = opStartTime;
				}
				if (opStartTime - startTime > WARMUP_TIME)
					valuesFreq.addValue(opTime);
			} else
				scanner.nextLine();
		}
		scanner.close();

		System.out.printf("LAT\tCUM_FREQ\n");
		for (int i = rangeI; i <= rangeF; i += increment) {
			System.out.printf("%d\t%f\n", i, valuesFreq.getCumPct(i));
		}

	}
	/**
	 * Transactions per second
	 * 
	 * @param rangeI
	 * @param rangeF
	 * @param increment
	 */
	public static void getTPSandTPL() {
		boolean warmUpComplete = false;
		long startTime = -1;
		long lastStart = -1;
		int count = 0;
		long totalLatencies = 0;

		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNext()) {
			if (scanner.hasNextLong()) {
				long opStartTime = scanner.nextLong();
				long opTime = scanner.nextLong();
				if (startTime == -1) {
					startTime = opStartTime;
				}
				if (!warmUpComplete && opStartTime - startTime > WARMUP_TIME) {
					warmUpComplete = true;
					startTime = opStartTime;
				}
				if (warmUpComplete) {
					if (opStartTime - startTime <= ONE_MINUTE_IN_NANOS) {
						count++;
						totalLatencies += opTime;
						lastStart = opStartTime;
					} else {
						// Just take results in one minute
						break;
					}
				}
			} else
				scanner.nextLine();
		}
		int duration = (int) ((lastStart - startTime) / ONE_SECOND_IN_NANOS);
		int TPS = count / duration;
		long AVGLatencyNanos = (totalLatencies / count);
		scanner.close();
		System.out.printf("START\tTPS\tTPL\n");
		System.out.printf("%s\t%s\t%s\n", startTime, TPS, AVGLatencyNanos);
	}

	public static void createTPS(String filter, String[] files) throws FileNotFoundException {
		System.out.printf("THREADS\tAVG_TPS\n");
		for (String file : files) {
			System.err.println("Processing file " + file);
			int idxI = file.indexOf(filter);
			int idxF = file.indexOf("-", idxI + 1);
			int nThreads = Integer.parseInt(file.substring(idxI + 2, idxF));
			long avg = average_column(1, file);
			System.out.printf("%s\t%s\n", nThreads, avg);
		}
	}

	private static void createLatencyForTPS(String filter, String[] files) throws FileNotFoundException {
		System.out.printf("THREADS\tTPS\tLAT\n");
		for (String file : files) {
			System.err.println("Processing file " + file);
			int idxI = file.indexOf(filter);
			int idxF = file.indexOf("-", idxI + 1);
			int nThreads = Integer.parseInt(file.substring(idxI + 2, idxF));
			long avgTP = average_column(1, file);
			long avgLat = average_column(2, file);
			System.out.printf("%s\t%s\t%s\n", nThreads, avgTP, avgLat);
		}
	}

	private static long average_column(int colIdx, String fileName) throws FileNotFoundException {
		File file = new File(fileName);
		Scanner fileIS = new Scanner(file);
		int count = 0;
		long sum = 0;

		// Skip header
		fileIS.nextLine();

		while (fileIS.hasNextLine()) {
			String[] lineToks = fileIS.nextLine().split("\t");
			sum += Long.parseLong(lineToks[colIdx]);
			count++;
		}
		fileIS.close();
		return sum / count;
	}

	// OP_NAME START_TIME DURATION SITE -> OP_NAME (MEAN_TIME, STD_DEV)*
	private static void createHistogramTournament(String[] filenames) throws FileNotFoundException {
		Map<String, Map<String, DescriptiveStatistics>> opsDuration = new HashMap<>();
		System.out.printf("OP_NAME\tUS-EAST\tUS-WEST\tEUROPE\tGLOBAL\n");

		for (String filename : filenames) {
			boolean warmUpComplete = false;
			long startTime = -1;

			File file = new File(filename);
			Scanner scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] tokens = line.split("\t");
				if (tokens.length != 7 || tokens[0].equals("OP_NAME")) {
					continue;
				}

				long opStartTime = Long.parseLong(tokens[1]);

				if (startTime == -1) {
					startTime = opStartTime;
				}
				if (!warmUpComplete && opStartTime - startTime > WARMUP_TIME) {
					warmUpComplete = true;
					startTime = opStartTime;
				}
				if (warmUpComplete) {
					if (opStartTime - startTime <= ONE_MINUTE_IN_NANOS) {
						opsDuration.putIfAbsent(tokens[0], new HashMap<>());
						Map<String, DescriptiveStatistics> op = opsDuration.get(tokens[0]);
						op.putIfAbsent(tokens[4], new DescriptiveStatistics());
						double d = Double.parseDouble(tokens[3]);
						if (d > OUTLIER) {
							System.err.println("ignored one line " + line);
							continue;
						}
						op.get(tokens[4]).addValue(d);
					} else {
						// Just take results in one minute
						break;
					}
				}
			}
			scanner.close();
		}

		String[] regions = {"US-EAST", "US-WEST", "EUROPE", "GLOBAL"};
		for (Entry<String, Map<String, DescriptiveStatistics>> op : opsDuration.entrySet()) {
			StringBuilder outputLine = new StringBuilder();
			outputLine.append(op.getKey());
			for (String region : regions) {
				DescriptiveStatistics values = op.getValue().getOrDefault(region, new DescriptiveStatistics());
				double mean = values.getMean();
				double stdDev = values.getStandardDeviation();
				double min = values.getMin();
				double max = values.getMax();
				outputLine.append(",");
				outputLine.append(mean);
				outputLine.append(",");
				outputLine.append(stdDev);
				outputLine.append(",");
				outputLine.append(min);
				outputLine.append(",");
				outputLine.append(max);
			}
			System.out.println(outputLine.toString());
		}
	}
	public static void main(String[] args) {
		try {
			if (args[0].equals("-cdf")) {
				int rangeI = Integer.parseInt(args[1]);
				int rangeF = Integer.parseInt(args[2]);
				int increment = Integer.parseInt(args[3]);
				getCDF(rangeI, rangeF, increment);
				return;
			}
			if (args[0].equals("-tpsl")) {
				getTPSandTPL();
			}
			if (args[0].equals("-tps")) {
				String filter = args[1];
				int numFiles = args.length - 2;
				String[] files = new String[numFiles];
				for (int i = 0; i < numFiles; i++) {
					files[i] = args[i + 2];
				}
				createTPS(filter, files);
			}
			if (args[0].equals("-latTPS")) {
				String filter = args[1];
				int numFiles = args.length - 2;
				String[] files = new String[numFiles];
				for (int i = 0; i < numFiles; i++) {
					files[i] = args[i + 2];
				}
				createLatencyForTPS(filter, files);
			}

			if (args[0].equals("-tourHist")) {
				int numFiles = args.length - 1;
				String[] files = new String[numFiles];
				for (int i = 0; i < numFiles; i++) {
					files[i] = args[i + 1];
				}
				createHistogramTournament(files);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

}
