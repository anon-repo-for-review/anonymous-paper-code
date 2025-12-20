package mathematical_relations;

import org.neo4j.procedure.*;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

public class QuantifyEventImpact_II {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    public static class EventImpactResult {
        public final String property;
        public final double peakImpact;
        public final long impactDuration;

        public EventImpactResult(String property, double peakImpact, long impactDuration) {
            this.property = property;
            this.peakImpact = peakImpact;
            this.impactDuration = impactDuration;
        }
    }

    @Procedure(name = "graphobs.analysis.quantify_event_impact", mode = Mode.READ)
    @Description("Berechnet Peak Impact und Impact Duration nach einem Event für eine univariate Zeitreihe.")
    public Stream<EventImpactResult> quantifyImpact(
            @Name("eventTime") String eventTimeStr,
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values,
            @Name(value = "windowMinutes", defaultValue = "60") long windowMinutes,
            @Name(value = "baselineWindowMinutes", defaultValue = "60") long baselineWindowMinutes,
            @Name(value = "thresholdSigma", defaultValue = "1.0") double thresholdSigma
    ) {
        ZonedDateTime eventTime = parseToZonedDateTime(eventTimeStr);

        // --- 1) Univariate Serie extrahieren ---
        if (values == null || values.size() != 1) {
            throw new IllegalArgumentException("Nur univariate Zeitreihen erlaubt. Die Map muss genau eine Serie enthalten.");
        }
        String propertyName = values.keySet().iterator().next();
        List<Double> series = values.get(propertyName);

        if (timestamps.size() != series.size()) {
            throw new IllegalArgumentException("timestamps und values müssen gleich lang sein.");
        }

        List<ZonedDateTime> parsedTimestamps = new ArrayList<>();
        for (String ts : timestamps) parsedTimestamps.add(parseToZonedDateTime(ts));

        List<Double> baseline = new ArrayList<>();
        List<Double> postEvent = new ArrayList<>();

        for (int i = 0; i < parsedTimestamps.size(); i++) {
            long minutes = Duration.between(eventTime, parsedTimestamps.get(i)).toMinutes();
            if (minutes >= -baselineWindowMinutes && minutes < 0) {
                baseline.add(series.get(i));
            } else if (minutes >= 0 && minutes <= windowMinutes) {
                postEvent.add(series.get(i));
            }
        }

        if (baseline.isEmpty() || postEvent.isEmpty()) {
            return Stream.empty();
        }

        double mean = baseline.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double stddev = Math.sqrt(baseline.stream().mapToDouble(v -> Math.pow(v - mean, 2)).average().orElse(1.0));
        double threshold = mean + thresholdSigma * stddev;

        double peak = postEvent.stream().mapToDouble(v -> v - mean).max().orElse(0.0);
        long duration = postEvent.stream().filter(v -> v > threshold).count();

        return Stream.of(new EventImpactResult(propertyName, peak, duration));
    }

    private ZonedDateTime parseToZonedDateTime(String s) {
        try {
            return ZonedDateTime.parse(s, FORMATTER);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid timestamp format: " + s);
        }
    }
}
