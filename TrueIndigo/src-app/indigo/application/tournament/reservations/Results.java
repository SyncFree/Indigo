package indigo.application.tournament.reservations;

import java.io.PrintStream;

public interface Results {

    Results setStartTime(long start);

    Results setSession(int sessionId);

    void logTo(PrintStream out);

    String getLogRecord();
}
