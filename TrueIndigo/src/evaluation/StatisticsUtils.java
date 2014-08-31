package evaluation;

import java.util.Scanner;

import org.apache.commons.math3.stat.Frequency;

public class StatisticsUtils {
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

		for (int i = rangeI; i <= rangeF; i += increment) {
			System.out.printf("%d\t%d\n", i, valuesFreq.getCumFreq(i));
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
	}

}
