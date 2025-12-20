package time_series_analysis;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import result_classes.TimePointsResult;
import result_classes.TimeSeriesResult;
import util.TimeSeriesUtil;

import java.util.*;
import java.util.stream.Stream;


public class OutlierDetection {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(name = "graphobs.analysis.detect_regression_outliers", mode = Mode.READ)
    @Description("Findet Zeitpunkte mit Ausreißern basierend auf lokaler linearer Regression im Gleitfenster.")
    public Stream<TimePointsResult> detectRegressionOutliers(
            @Name("tsNode") Node tsNode,
            @Name("metric") String metric,
            @Name("windowSize") long windowSize,
            @Name("residuumThreshold") double threshold) {


        String[] timestamps = (String[]) tsNode.getProperty("timestamps");
        int n = timestamps.length;
        int w = (int) windowSize;

        if (w < 3 || w > n || w % 2 == 0) {
            throw new IllegalArgumentException("windowSize muss ungerade, ≥3 und ≤ Zeitreihenlänge sein.");
        }

        int half = w / 2;

        // heir ändern !!!
        TimeSeriesResult tsResult = TimeSeriesUtil.getFilteredTimeSeries(tsNode, metric, Map.of(), db, log).toList().get(0);

        Map<String, List<Double>> valuesMap = tsResult.values;


        Map<String, List<String>> outlierTimestamps = new HashMap<>();

        for (Map.Entry<String, List<Double>> entry : valuesMap.entrySet()) {
            String key = entry.getKey();
            List<Double> rawVals = entry.getValue();

            double[] y = rawVals.stream()
                    .mapToDouble(o -> ((Number) o).doubleValue())
                    .toArray();

            List<String> outlierTimes = new ArrayList<>();

            for (int i = half; i < n - half; i++) {
                // Fenster x und y aufbauen
                double[] xWin = new double[w];
                double[] yWin = new double[w];

                for (int j = 0; j < w; j++) {
                    xWin[j] = i - half + j;
                    yWin[j] = y[i - half + j];
                }

                // Regression
                double xMean = Arrays.stream(xWin).average().orElse(0);
                double yMean = Arrays.stream(yWin).average().orElse(0);

                double num = 0, den = 0;
                for (int j = 0; j < w; j++) {
                    num += (xWin[j] - xMean) * (yWin[j] - yMean);
                    den += (xWin[j] - xMean) * (xWin[j] - xMean);
                }

                double slope = (den != 0) ? num / den : 0;
                double intercept = yMean - slope * xMean;

                double predicted = slope * i + intercept;
                double residuum = Math.abs(y[i] - predicted);

                if (residuum > threshold) {
                    outlierTimes.add(timestamps[i]);
                }
            }

            if (!outlierTimes.isEmpty()) {
                outlierTimestamps.put(key, outlierTimes);
            }
        }

        return Stream.of(new TimePointsResult(outlierTimestamps));
    }
}
