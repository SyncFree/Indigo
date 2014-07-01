package sys.utils;

public class NotImplemented extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public final static RuntimeException NotImplemented = new NotImplemented();

	NotImplemented() {
		super("not implemented...");
	}
}
