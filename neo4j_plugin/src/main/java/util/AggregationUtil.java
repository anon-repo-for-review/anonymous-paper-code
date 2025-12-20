package util;

import aggregation_functions.*;
import org.neo4j.logging.Log;
import result_classes.TimeSeriesResult;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class AggregationUtil {

    private AggregationUtil() {
        // Utility-Klasse → kein Konstruktor nach außen
    }

    public static Stream<TimeSeriesResult> apply(
            String aggregation,
            TimeSeriesResult raw,
            Integer period,
            Log log
    ) {
        if (aggregation == null || aggregation.isBlank()) {
            // Keine Aggregation → einfach Rohdaten zurückgeben
            return Stream.of(raw);
        }

        List<String> timestamps = raw.timestamps;
        Map<String, List<Double>> values = raw.values;

        switch (aggregation) {
            case "binned_average":
                return Stream.of(
                        BinnedAverage.calc_binned_average(
                                timestamps.toArray(new String[0]),
                                values,
                                period
                        )
                );

            case "cu_sum":
                return Stream.of(
                        CumulativeSum_Integral.calc_cu_sum(
                                timestamps.toArray(new String[0]),
                                values
                        )
                );

            case "integral":
                return Stream.of(
                        CumulativeSum_Integral.calc_integral(
                                timestamps.toArray(new String[0]),
                                values
                        )
                );

            case "difference":
                return Stream.of(
                        Difference_Derivative.calc_difference(
                                timestamps.toArray(new String[0]),
                                values
                        )
                );

            case "derivative":
                return Stream.of(
                        Difference_Derivative.calc_derivative(
                                timestamps.toArray(new String[0]),
                                values
                        )
                );

            case "linear_regression":
                return Stream.of(
                        LinerRegression.calc_linear_regression(
                                timestamps.toArray(new String[0]),
                                values
                        )
                );

            case "moving_average":
                return Stream.of(
                        MovingAverage.calc_moving_average(
                                timestamps.toArray(new String[0]),
                                values,
                                period
                        )
                );

            default:
                log.warn(
                        "Unknown aggregation type: '%s'. Returning raw data without aggregation.",
                        aggregation
                );
                return Stream.of(raw);
        }
    }
}
