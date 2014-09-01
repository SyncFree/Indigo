package evaluation;

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.math3.stat.Frequency;

import swift.utils.Pair;

public class StatisticsUtils {
	private static final long ONE_SECOND_IN_MILLIS = 1000;
	static Frequency valuesFreq;

	public static void getCDF(int rangeI, int rangeF, int increment) {
		valuesFreq = new Frequency();
		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNext()) {
			if (scanner.hasNextInt()) {
				int opTime = scanner.nextInt();
				valuesFreq.addValue(opTime);
			} else
				scanner.nextLine();
		}
		scanner.close();

		System.out.printf("LAT\tCUM_FREQ\n");
		for (int i = rangeI; i <= rangeF; i += increment) {
			System.out.printf("%d\t%d\n", i, valuesFreq.getCumFreq(i));
		}

	}
	/**
	 * Transactions per minute and Throughput/Latency
	 * 
	 * @param rangeI
	 * @param rangeF
	 * @param increment
	 */
	public static void getTPSAndTPL() {
		long lastSecond = 0;
		int count = 0;
		long sumLatencies = 0;
		List<Pair<Long, Integer>> tpMinute = new LinkedList<Pair<Long, Integer>>();
		List<Pair<Integer, Long>> tpLatency = new LinkedList<Pair<Integer, Long>>();
		Scanner scanner = new Scanner(System.in);
		while (scanner.hasNext()) {
			if (scanner.hasNextLong()) {
				long startTime = scanner.nextLong();
				long opTime = scanner.nextLong();
				sumLatencies += opTime;
				if (startTime - lastSecond > ONE_SECOND_IN_MILLIS) {
					tpMinute.add(new Pair<>(startTime, count));
					if (count != 0) {
						tpLatency.add(new Pair<Integer, Long>(count, sumLatencies / count));
					} else {
						tpLatency.add(new Pair<Integer, Long>(0, 0l));
					}

					count = 0;
					sumLatencies = 0;
					lastSecond = startTime;
				}
				count++;
			} else
				scanner.nextLine();
		}
		scanner.close();
		System.out.printf("START\tTPS\tTPL\n");
		while (!tpMinute.isEmpty()) {
			Pair<Long, Integer> tpm = tpMinute.remove(0);
			Pair<Integer, Long> tpl = tpLatency.remove(0);
			System.out.printf("%s\t%s\t%s\n", tpm.getFirst(), tpm.getSecond(), tpl.getSecond());
		}
	}

	public static void main(String[] args) {
		if (args[0].equals("-cdf")) {
			int rangeI = Integer.parseInt(args[1]);
			int rangeF = Integer.parseInt(args[2]);
			int increment = Integer.parseInt(args[3]);
			getCDF(rangeI, rangeF, increment);
			return;
		}
		if (args[0].equals("-tpsl")) {
			getTPSAndTPL();
		}
	}

}