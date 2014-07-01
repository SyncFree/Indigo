package sys.api;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public interface Sys {

	String context();

	long timeMillis();

	double currentTime();

	Random random();

	AtomicLong uploadedBytes();

	AtomicLong downloadedBytes();
}
