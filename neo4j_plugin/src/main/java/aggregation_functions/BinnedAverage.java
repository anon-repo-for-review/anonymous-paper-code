package aggregation_functions;


import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

//import static util.TimeSeriesUtil.getDoubleTs;
//import static util.TimeSeriesUtil.getValueProperties;

public class BinnedAverage {




    @Procedure(name = "graphobs.aggregation.binned_average", mode = Mode.READ)
    @Description("Berechnet gleitenden Durchschnitt in regelmäßigen Zeitintervallen (z. B. alle 5 Sekunden) für eine Property oder alle Properties.")
    public Stream<TimeSeriesResult> binnedAverage(
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values,
            @Name(value = "window", defaultValue = "5") long w_size
    ){
        TimeSeriesResult ts_result  = calc_binned_average(timestamps.toArray(new String[0]), values, w_size);

        return Stream.of(ts_result);
    }



    private static List<ZonedDateTime> generateBins(ZonedDateTime[] timestamps, long intervalSeconds) {
        ZonedDateTime start = Arrays.stream(timestamps).min(ZonedDateTime::compareTo).orElseThrow();
        ZonedDateTime end = Arrays.stream(timestamps).max(ZonedDateTime::compareTo).orElseThrow();

        List<ZonedDateTime> bins = new ArrayList<>();
        ZonedDateTime current = start;
        while (!current.isAfter(end)) {
            bins.add(current);
            current = current.plusSeconds(intervalSeconds);
        }
        return bins;
    }





    public static TimeSeriesResult calc_binned_average(String[] timestampStrs, Map<String, List<Double>> valueMap, long intervalSeconds){
        ZonedDateTime[] timestamps = Arrays.stream(timestampStrs)
                .map(TimeSeriesUtil::parseToZonedDateTime)
                .toArray(ZonedDateTime[]::new);

        List<ZonedDateTime> binStarts = generateBins(timestamps, intervalSeconds);
        Map<String, List<Double>> binnedValues = new HashMap<>();
        for (String key : valueMap.keySet()) {
            //if (!property.isEmpty() && !property.equals(key)) continue;
            List<Double> values = valueMap.get(key);
            List<Double> means = new ArrayList<>();

            for (ZonedDateTime binStart : binStarts) {
                ZonedDateTime binEnd = binStart.plusSeconds(intervalSeconds);
                List<Double> binData = new ArrayList<>();

                for (int i = 0; i < timestamps.length; i++) {
                    if (!timestamps[i].isBefore(binStart) && timestamps[i].isBefore(binEnd)) {
                        Object val = values.get(i);
                        if (val instanceof Number) {
                            binData.add(((Number) val).doubleValue());
                        }
                    }
                }

                double avg = binData.isEmpty() ? 0.0 : binData.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                means.add(avg);
            }
            binnedValues.put(key, means);
        }

        List<String> binStartStrs = binStarts.stream().map(ZonedDateTime::toString).toList();
        return new TimeSeriesResult(binStartStrs, binnedValues);
    }


}

