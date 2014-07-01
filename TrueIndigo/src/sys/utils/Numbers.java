package sys.utils;

public class Numbers {

	public static boolean isInteger(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (Exception x) {
			return false;
		}
	}

	public static int integer(String s) {
		return Integer.parseInt(s);
	}
}
