package indigo.application.adservice;

import java.io.PrintStream;

public interface Results {

    Results setStartTime(long start);

    Results setSession(int sessionId);

    void logTo(PrintStream out);

    String getLogRecord();
}
