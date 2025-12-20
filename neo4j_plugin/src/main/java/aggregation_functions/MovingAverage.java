package aggregation_functions;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.*;

//import static util.TimeSeriesUtil.getValueProperties;

public class MovingAverage {

    @Context
    public GraphDatabaseService db;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME;



    @Procedure(name = "graphobs.aggregation.moving_average", mode = Mode.READ)
    public Stream<TimeSeriesResult> movingAverageByPoints(
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values,
            @Name(value = "window", defaultValue = "5") long w_size
    ) {
        TimeSeriesResult timeSeriesResult = calc_moving_average(timestamps.toArray(new String[0]), values, w_size);
        return Stream.of(timeSeriesResult);
    }


    public static TimeSeriesResult calc_moving_average(String[] timestampStrs, Map<String, List<Double>> valueMap, long windowSize){
        Map<String, List<Double>> resultValues = new HashMap<>();
        List<String> resultTimestamps = new ArrayList<>();

        for (Map.Entry<String, List<Double>> entry : valueMap.entrySet()) {
            String key = entry.getKey();
            List<Double> values = entry.getValue();
            List<Double> movingAvg = new ArrayList<>();

            for (int i = 0; i <= values.size() - windowSize; i++) {
                double sum = 0;
                for (int j = 0; j < windowSize; j++) {
                    sum += values.get(i + j);
                }
                movingAvg.add(sum / windowSize);
                if (resultTimestamps.size() < movingAvg.size()) {
                    resultTimestamps.add(timestampStrs[(int) (i + windowSize - 1)]);
                }
            }
            resultValues.put(key, movingAvg);
        }

        return new TimeSeriesResult(resultTimestamps, resultValues);
    }
}