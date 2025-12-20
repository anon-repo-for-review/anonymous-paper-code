package aggregation_functions;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

import static util.TimeSeriesUtil.*;

public class Difference_Derivative {



    @Procedure(name = "graphobs.aggregation.difference", mode = Mode.READ)
    @Description("Berechnet die Differenz zwischen aufeinanderfolgenden Werten für eine Property oder alle Properties.")
    public Stream<TimeSeriesResult> difference(
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values
    ) {
        TimeSeriesResult timeSeriesResult = calc_difference(timestamps.toArray(new String[0]), values);
        return Stream.of(timeSeriesResult);
    }


    public static TimeSeriesResult calc_difference(String[] timestampStrs, Map<String, List<Double>> valueMap){
        ZonedDateTime[] timestamps = Arrays.stream(timestampStrs)
                .map(TimeSeriesUtil::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);

        Map<String, List<Double>> differences = new HashMap<>();
        for (String key : valueMap.keySet()) {

            List<Double> values = valueMap.get(key);
            List<Double> diffs = new ArrayList<>();

            for (int i = 1; i < values.size(); i++) {
                double diff = values.get(i)- values.get(i - 1);
                diffs.add(diff);
            }
            differences.put(key, diffs);
        }

        List<String> trimmedTimestamps = Arrays.stream(timestamps)
                .skip(1)
                .map(ZonedDateTime::toString)
                .toList();

        return new TimeSeriesResult(trimmedTimestamps, differences);
    }


    /*@Procedure(name = "decrapted.aggregation.derivative", mode = Mode.READ)
    @Description("Berechnet die Änderungsrate pro Sekunde (erste Ableitung) zwischen Zeitpunkten für eine Property oder alle Properties.")
    public Stream<TimeSeriesResult> derivative(
            @Name("timeSeries") Node tsNode,
            @Name(value = "property", defaultValue = "") String property
    ) {
        String[] timestampStrs = (String[]) tsNode.getProperty("timestamps", new String[0]);
        if (timestampStrs.length < 2) return Stream.empty();

        ZonedDateTime[] timestamps = Arrays.stream(timestampStrs)
                .map(TimeSeriesUtil::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);

        Map<String, Object[]> valueMap = getValueProperties(tsNode, timestamps.length);

        Map<String, List<Double>> derivatives = new HashMap<>();
        for (String key : valueMap.keySet()) {
            if (!property.isEmpty() && !property.equals(key)) continue;

            Object[] values = valueMap.get(key);
            List<Double> ders = new ArrayList<>();

            for (int i = 1; i < values.length; i++) {
                if (values[i] instanceof Number && values[i - 1] instanceof Number) {
                    double deltaVal = ((Number) values[i]).doubleValue() - ((Number) values[i - 1]).doubleValue();
                    long deltaTimeSec = Duration.between(timestamps[i - 1], timestamps[i]).toMillis();
                    if (deltaTimeSec == 0) deltaTimeSec = 1; // Schutz gegen Division durch 0
                    double der = deltaVal / (deltaTimeSec / 1000.0);
                    ders.add(der);
                } else {
                    ders.add(0.0);
                }
            }
            derivatives.put(key, ders);
        }

        List<String> trimmedTimestamps = Arrays.stream(timestamps)
                .skip(1)
                .map(ZonedDateTime::toString)
                .toList();

        return Stream.of(new TimeSeriesResult(trimmedTimestamps, derivatives));
    }*/


    @Procedure(name = "graphobs.aggregation.derivative", mode = Mode.READ)
    @Description("Berechnet die Änderungsrate pro Sekunde (erste Ableitung) zwischen Zeitpunkten für eine Property oder alle Properties.")
    public Stream<TimeSeriesResult> derivative(
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values
    ) {
        TimeSeriesResult timeSeriesResult = calc_derivative(timestamps.toArray(new String[0]), values);
        return Stream.of(timeSeriesResult);
    }

    public static TimeSeriesResult calc_derivative(String[] timestampStrs, Map<String, List<Double>> valueMap) {

        ZonedDateTime[] timestamps = Arrays.stream(timestampStrs)
                .map(TimeSeriesUtil::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);


        Map<String, List<Double>> derivatives = new HashMap<>();
        for (String key : valueMap.keySet()) {

            List<Double> values = valueMap.get(key);
            List<Double> ders = new ArrayList<>();

            for (int i = 1; i < values.size(); i++) {
                double deltaVal = values.get(i)- values.get(i - 1);
                long deltaTimeSec = Duration.between(timestamps[i - 1], timestamps[i]).toMillis();
                if (deltaTimeSec == 0) deltaTimeSec = 1; // Schutz gegen Division durch 0
                double der = deltaVal / (deltaTimeSec / 1000.0);
                ders.add(der);

            }
            derivatives.put(key, ders);
        }

        List<String> trimmedTimestamps = Arrays.stream(timestamps)
                .skip(1)
                .map(ZonedDateTime::toString)
                .toList();

        return new TimeSeriesResult(trimmedTimestamps, derivatives);
    }
}
