package util;

import java.time.Instant;

public final class TimeWindow {
    public final long startTime;
    public final long endTime;

    public TimeWindow(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "TimeWindow[" + Instant.ofEpochMilli(startTime) + " -> " + Instant.ofEpochMilli(endTime) + "]";
    }
}
