package aggregation_functions;

import org.neo4j.graphdb.Node;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.util.*;
import java.util.stream.Stream;

public class LinerRegression {


    @Procedure(name = "graphobs.aggregation.linear_regression", mode = Mode.READ)
    @Description("Berechnet die Werte der linearen Regression (ŷ = a·x + b) für jede Zeitreihe")
    public Stream<TimeSeriesResult> linearRegressionValues(
            @Name("timestamps") List<String> timestamps,
            @Name("values") Map<String, List<Double>> values
    ) {
        TimeSeriesResult timeSeriesResult = calc_linear_regression(timestamps.toArray(new String[0]), values);
        return Stream.of(timeSeriesResult);
    }


    public static TimeSeriesResult calc_linear_regression(String[] timestampStrs, Map<String, List<Double>> valueMap){
        Map<String, List<Double>> regressionValues = new HashMap<>();
        int n = timestampStrs.length;

        for (Map.Entry<String, List<Double>> entry : valueMap.entrySet()) {
            List<Double> yValuesRaw = entry.getValue();
            double[] y = yValuesRaw.stream()
                    .mapToDouble(o -> ((Number) o).doubleValue())
                    .toArray();

            // Berechne lineare Regression: y = a·x + b
            double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            for (int i = 0; i < n; i++) {
                double x = i;
                double yi = y[i];

                sumX += x;
                sumY += yi;
                sumXY += x * yi;
                sumX2 += x * x;
            }

            double slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
            double intercept = (sumY - slope * sumX) / n;

            // Berechne Regressionswerte für alle Zeitpunkte
            List<Double> fitted = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                double x = i;
                double yFit = slope * x + intercept;
                fitted.add(yFit);
            }

            regressionValues.put(entry.getKey(), fitted);
        }

        return new TimeSeriesResult(List.of(timestampStrs), regressionValues);
    }

}
