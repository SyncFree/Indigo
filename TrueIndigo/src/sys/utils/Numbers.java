package sys.utils;

final public class Numbers {

	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (Exception x) {
			return false;
		}
	}

	public static boolean isOdd(long v) {
		return (v & 1L) != 0L;
	}

	public static int integer(String s) {
		return Integer.parseInt(s);
	}
}
