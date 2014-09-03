package evaluation;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.commons.math3.stat.Frequency;

public class StatisticsUtils {
	private static final long ONE_SECOND_IN_NANOS = 1000000000;
	private static final long ONE_MINUTE_IN_NANOS = ONE_SECOND_IN_NANOS * 60;
	private static final long WARMUP_TIME = ONE_SECOND_IN_NANOS * 20;
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
					} else {
						// Just take results in one minute
						break;
					}
				}
			} else
				scanner.nextLine();
		}
		int TPS = count / 60;
		long AVGLatencyNanos = (totalLatencies / count);
		scanner.close();
		System.out.printf("START\tTPS\tTPL\n");
		System.out.printf("%s\t%s\t%s\n", startTime, TPS, AVGLatencyNanos);
	}

	public static void createTPS(String filter, String[] files) throws FileNotFoundException {
		System.out.printf("THREADS\tAVG_TPS\n");
		for (String file : files) {
			int idxI = file.indexOf("-t");
			int idxF = file.indexOf("-", idxI + 1);
			int nThreads = Integer.parseInt(file.substring(idxI + 2, idxF));
			long avg = average_column(1, file);
			System.out.printf("%s\t%s\n", nThreads, avg);
		}
	}

	private static void createLatencyForTPS(String filter, String[] files) throws FileNotFoundException {
		System.out.printf("TPS\tLAT\n");
		for (String file : files) {
			int idxI = file.indexOf(filter);
			int idxF = file.indexOf("-", idxI);
			int nThreads = Integer.parseInt(file.substring(idxI, idxF));
			long avg = average_column(2, file);
			System.out.printf("%s\t%s\n", nThreads, avg);
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

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

}
