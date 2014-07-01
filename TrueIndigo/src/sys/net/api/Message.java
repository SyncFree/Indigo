package sys.net.api;

public interface Message {

	default void deliverTo(final Envelope sender, final MessageHandler handler) {
		Thread.dumpStack();
	}

}
