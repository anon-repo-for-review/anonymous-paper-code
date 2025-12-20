package aggregation_functions;

import org.neo4j.graphdb.Node;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Stream;

//import static util.TimeSeriesUtil.getValueProperties;

public class CumulativeSum_Integral {


    @Procedure(name = "graphobs.aggregation.cumulative_sum", mode = Mode.READ)
    @Description("Berechnet die kumulierte Summe für ein Werte-Array. Optional auf ein Property beschränkbar.")
    public Stream<TimeSeriesResult> cumulativeSum(
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values
    ) {
        TimeSeriesResult ts_r = calc_cu_sum(timestamps.toArray(new String[0]), values);
        return Stream.of(ts_r);
    }


    public static TimeSeriesResult calc_cu_sum(String[] timestampStrs, Map<String, List<Double>> valueMap) {
        ZonedDateTime[] timestamps = Arrays.stream(timestampStrs)
                .map(TimeSeriesUtil::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);

        Map<String, List<Double>> cumulativeSums = new HashMap<>();

        for (String key : valueMap.keySet()) {
            List<Double> values = valueMap.get(key);
            List<Double> cumSumList = new ArrayList<>();
            double sum = 0.0;

            for (int i = 1; i < values.size(); i++) {
                sum += values.get(i);
                cumSumList.add(sum);
            }

            cumulativeSums.put(key, cumSumList);
        }

        List<String> resultTimestamps = Arrays.stream(timestamps)
                .skip(1)
                .map(ZonedDateTime::toString)
                .toList();

        return new TimeSeriesResult(resultTimestamps, cumulativeSums);
    }




    /*public static List<Double> calc_cum_sum(List<Double> values){
            List<Double> cumulative = new ArrayList<>();
            double sum = 0.0;

            for (Double val : values) {
                sum += val;
                cumulative.add(sum);
            }
        return cumulative;
    }*/


    /*@Procedure(name = "decrapted.aggregation.integral", mode = Mode.READ)
    @Description("Berechnet das Integral (Wert × Zeitabstand) zwischen Punkten in Sekunden – wie InfluxDB.")
    public Stream<TimeSeriesResult> integral(
            @Name("timeSeries") Node tsNode,
            @Name(value = "property", defaultValue = "") String property
    ) {
        String[] timestampStrs = (String[]) tsNode.getProperty("timestamps", new String[0]);
        if (timestampStrs.length < 2) return Stream.empty();

        ZonedDateTime[] timestamps = Arrays.stream(timestampStrs)
                .map(TimeSeriesUtil::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);

        Map<String, Object[]> valueMap = getValueProperties(tsNode, timestamps.length);
        Map<String, List<Double>> integrals = new HashMap<>();

        for (String key : valueMap.keySet()) {
            if (!property.isEmpty() && !property.equals(key)) continue;

            Object[] values = valueMap.get(key);
            List<Double> intList = new ArrayList<>();
            double integral = 0.0;

            for (int i = 1; i < values.length; i++) {
                if (values[i - 1] instanceof Number) {
                    double v = ((Number) values[i - 1]).doubleValue();
                    long dtMillis = Duration.between(timestamps[i - 1], timestamps[i]).toMillis();
                    integral += v * (dtMillis / 1000.0); // in Sekunden umrechnen

                }
                intList.add(integral);
            }

            integrals.put(key, intList);
        }

        List<String> resultTimestamps = Arrays.stream(timestamps)
                .skip(1)
                .map(ZonedDateTime::toString)
                .toList();

        return Stream.of(new TimeSeriesResult(resultTimestamps, integrals));
    }*/


    @Procedure(name = "graphobs.aggregation.integral", mode = Mode.READ)
    @Description("Berechnet das Integral (Wert × Zeitabstand) zwischen Punkten in Sekunden – wie InfluxDB.")
    public Stream<TimeSeriesResult> integral(
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values
    ) {
        TimeSeriesResult timeSeriesResult = calc_integral(timestamps.toArray(new String[0]), values);
        return Stream.of(timeSeriesResult);
    }


    public static TimeSeriesResult calc_integral(String[] timestampStrs, Map<String, List<Double>> valueMap){
        ZonedDateTime[] timestamps = Arrays.stream(timestampStrs)
                .map(TimeSeriesUtil::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);


        Map<String, List<Double>> integrals = new HashMap<>();

        for (String key : valueMap.keySet()) {

            List<Double> values = valueMap.get(key);
            List<Double> intList = new ArrayList<>();
            double integral = 0.0;

            for (int i = 1; i < values.size(); i++) {
                double v = values.get(i - 1);
                long dtMillis = Duration.between(timestamps[i - 1], timestamps[i]).toMillis();
                integral += v * (dtMillis / 1000.0); // in Sekunden umrechnen

                intList.add(integral);
            }

            integrals.put(key, intList);
        }

        List<String> resultTimestamps = Arrays.stream(timestamps)
                .skip(1)
                .map(ZonedDateTime::toString)
                .toList();

        return new TimeSeriesResult(resultTimestamps, integrals);
    }






}
