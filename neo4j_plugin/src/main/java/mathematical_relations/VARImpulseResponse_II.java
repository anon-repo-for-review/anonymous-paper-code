package mathematical_relations;

import org.neo4j.procedure.*;
import result_classes.TimeSeriesResult;
import temporal_joins.AlignedData;
import temporal_joins.JoinStrategyFactory;
import temporal_joins.TemporalJoinStrategy;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

public class VARImpulseResponse_II {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

    public static class IRFVectorResult {
        public List<Double> responseY1;
        public List<Double> responseY2;

        public IRFVectorResult(List<Double> r1, List<Double> r2) {
            this.responseY1 = r1;
            this.responseY2 = r2;
        }
    }

    @Procedure(name = "graphobs.analysis.var_irf", mode = Mode.READ)
    @Description("Berechnet ein VAR(1)-Modell mit IRF aus zwei univariaten Zeitreihen, übergeben als (timestamps, values).")
    public Stream<IRFVectorResult> computeVAR_IRF(
            @Name("timestamps1") List<String> timestamps1,
            @Name("values1") Map<String, List<Double>> values1,
            @Name("timestamps2") List<String> timestamps2,
            @Name("values2") Map<String, List<Double>> values2,
            //@Name("intervalSeconds") long intervalSeconds,
            //@Name("steps") long steps,
            @Name(value = "params", defaultValue = "{}") Map<String, Object> params
    ) {
        String metricKey1 = getAndValidateMetricKey(values1, "values1");
        String metricKey2 = getAndValidateMetricKey(values2, "values2");

        long steps = (long) params.getOrDefault("steps", 10);

        List<TimeSeriesResult> seriesList1 = Collections.singletonList(new TimeSeriesResult(timestamps1, values1));
        List<TimeSeriesResult> seriesList2 = Collections.singletonList(new TimeSeriesResult(timestamps2, values2));

        // --- 2) Temporal Join via Resampling-Strategie ---
        TemporalJoinStrategy joinStrategy = JoinStrategyFactory.getStrategy((String) params.getOrDefault("join", "linear"));
        Map<String, Object> join_params = new HashMap<>();
        join_params.put("intervalSeconds", params.getOrDefault("intervalSeconds", 60));

        AlignedData alignedData = joinStrategy.align(seriesList1, metricKey1, seriesList2, metricKey2, join_params);

        if (alignedData.size() < 4) { // VAR(1) benötigt mind. 3 Beobachtungen + 1 lag
            return Stream.empty();
        }

        // --- 3) Regressionsmatrizen aus den ausgerichteten Daten aufbauen ---
        int numObs = alignedData.size() - 1;
        double[][] X = new double[numObs][3];
        double[] Y1 = new double[numObs];
        double[] Y2 = new double[numObs];

        for (int i = 0; i < numObs; i++) {
            X[i][0] = 1.0; // Intercept
            X[i][1] = alignedData.valuesA[i]; // y1_lag
            X[i][2] = alignedData.valuesB[i]; // y2_lag
            Y1[i] = alignedData.valuesA[i + 1];
            Y2[i] = alignedData.valuesB[i + 1];
        }



        // --- 1) Univariate Zeitreihen extrahieren ---
        /*List<Double> seriesList1 = extractSingleSeries(values1, "values1");
        List<Double> seriesList2 = extractSingleSeries(values2, "values2");

        if (timestamps1.size() != seriesList1.size())
            throw new IllegalArgumentException("timestamps1 und values1 müssen gleich lang sein.");
        if (timestamps2.size() != seriesList2.size())
            throw new IllegalArgumentException("timestamps2 und values2 müssen gleich lang sein.");

        // --- 2) Zeitstempel parsen und Werte aggregieren nach intervalSeconds ---
        Map<ZonedDateTime, Double> series1 = aggregateSeries(timestamps1, seriesList1, intervalSeconds);
        Map<ZonedDateTime, Double> series2 = aggregateSeries(timestamps2, seriesList2, intervalSeconds);

        // --- 3) Gemeinsame Zeitstempel bestimmen ---
        List<ZonedDateTime> commonTimestamps = new ArrayList<>(series1.keySet());
        commonTimestamps.retainAll(series2.keySet());
        Collections.sort(commonTimestamps);

        if (commonTimestamps.size() < 4) {
            return Stream.empty();
        }

        int n = commonTimestamps.size();
        int numObs = n - 1;

        // --- 4) Regressionsmatrizen aufbauen ---
        double[][] X = new double[numObs][3];
        double[] Y1 = new double[numObs];
        double[] Y2 = new double[numObs];

        for (int i = 0; i < numObs; i++) {
            X[i][0] = 1.0; // Intercept
            X[i][1] = series1.get(commonTimestamps.get(i)); // y1_lag
            X[i][2] = series2.get(commonTimestamps.get(i)); // y2_lag
            Y1[i] = series1.get(commonTimestamps.get(i + 1));
            Y2[i] = series2.get(commonTimestamps.get(i + 1));
        }*/




        // --- 5) OLS für jede Gleichung ---
        double[] beta1 = solveOLS(X, Y1);
        double[] beta2 = solveOLS(X, Y2);

        double[][] B = {
                {beta1[1], beta1[2]},
                {beta2[1], beta2[2]}
        };

        // --- 6) Residuen berechnen ---
        double[] residuals1 = new double[numObs];
        double[] residuals2 = new double[numObs];
        for (int i = 0; i < numObs; i++) {
            double predicted1 = X[i][0] * beta1[0] + X[i][1] * beta1[1] + X[i][2] * beta1[2];
            double predicted2 = X[i][0] * beta2[0] + X[i][1] * beta2[1] + X[i][2] * beta2[2];
            residuals1[i] = Y1[i] - predicted1;
            residuals2[i] = Y2[i] - predicted2;
        }

        // --- 7) Kovarianz & Cholesky ---
        double[][] sigma = computeCovarianceMatrix(residuals1, residuals2);
        double[][] P = cholesky(sigma);

        // --- 8) IRF berechnen ---
        List<double[]> irfSteps = new ArrayList<>();
        double[][] B_power_h = identity(2);

        for (int h = 0; h <= steps; h++) {
            if (h > 0) {
                B_power_h = multiply(B, B_power_h);
            }
            double[][] responseMatrix = multiply(B_power_h, P);
            irfSteps.add(new double[]{responseMatrix[0][0], responseMatrix[1][0]});
        }

        List<Double> responseVector1 = new ArrayList<>();
        List<Double> responseVector2 = new ArrayList<>();
        for (double[] stepResult : irfSteps) {
            responseVector1.add(stepResult[0]);
            responseVector2.add(stepResult[1]);
        }

        return Stream.of(new IRFVectorResult(responseVector1, responseVector2));
    }

    // ---------------- Hilfsfunktionen ----------------

    private static List<Double> extractSingleSeries(Map<String, List<Double>> valuesMap, String paramName) {
        if (valuesMap == null || valuesMap.isEmpty()) {
            throw new IllegalArgumentException(paramName + " darf nicht leer sein und muss genau eine Serie enthalten.");
        }
        if (valuesMap.size() != 1) {
            throw new IllegalArgumentException(paramName + " enthält " + valuesMap.size() +
                    " Serien. Nur univariate Zeitreihen (genau eine Serie) sind erlaubt.");
        }
        return valuesMap.values().iterator().next();
    }

    /*private Map<ZonedDateTime, Double> aggregateSeries(List<String> timestamps, List<Double> values, long intervalSeconds) {
        Map<ZonedDateTime, List<Double>> bucketMap = new HashMap<>();

        for (int i = 0; i < timestamps.size(); i++) {
            ZonedDateTime ts = parseToZonedDateTime(timestamps.get(i));
            long bucketStart = (ts.toEpochSecond() / intervalSeconds) * intervalSeconds;
            ZonedDateTime bucket = ZonedDateTime.ofInstant(Instant.ofEpochSecond(bucketStart), ts.getZone());
            bucketMap.computeIfAbsent(bucket, k -> new ArrayList<>()).add(values.get(i));
        }

        Map<ZonedDateTime, Double> aggregated = new HashMap<>();
        for (Map.Entry<ZonedDateTime, List<Double>> e : bucketMap.entrySet()) {
            double avg = e.getValue().stream().mapToDouble(Double::doubleValue).average().orElse(0);
            aggregated.put(e.getKey(), avg);
        }
        return aggregated;
    }

    private ZonedDateTime parseToZonedDateTime(String s) {
        try { return ZonedDateTime.parse(s, FORMATTER); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid timestamp format: " + s); }
    }*/

    private double[] solveOLS(double[][] X, double[] y) {
        double[][] Xt = transpose(X);
        double[][] XtX = multiply(Xt, X);
        double[][] XtX_inv = inverse(XtX);
        if (XtX_inv == null) throw new IllegalStateException("Matrix X'X ist singulär.");
        double[] Xty = multiply(Xt, y);
        return multiply(XtX_inv, Xty);
    }

    private double[][] transpose(double[][] matrix) {
        int rows = matrix.length, cols = matrix[0].length;
        double[][] transposed = new double[cols][rows];
        for (int i = 0; i < rows; i++) for (int j = 0; j < cols; j++) transposed[j][i] = matrix[i][j];
        return transposed;
    }

    private double[][] multiply(double[][] A, double[][] B) {
        int aRows = A.length, aCols = A[0].length, bRows = B.length, bCols = B[0].length;
        if (aCols != bRows) throw new IllegalArgumentException("Matrix dimensions incompatible.");
        double[][] result = new double[aRows][bCols];
        for (int i = 0; i < aRows; i++) for (int j = 0; j < bCols; j++)
            for (int k = 0; k < aCols; k++) result[i][j] += A[i][k] * B[k][j];
        return result;
    }

    private double[] multiply(double[][] A, double[] x) {
        int rows = A.length, cols = A[0].length;
        if (cols != x.length) throw new IllegalArgumentException("Matrix- und Vektor-Dimensionen inkompatibel.");
        double[] result = new double[rows];
        for (int i = 0; i < rows; i++) for (int j = 0; j < cols; j++) result[i] += A[i][j] * x[j];
        return result;
    }

    private double[][] identity(int size) {
        double[][] id = new double[size][size];
        for (int i = 0; i < size; i++) id[i][i] = 1.0;
        return id;
    }

    private double[][] inverse(double[][] m) {
        if (m.length != 3 || m[0].length != 3) throw new IllegalArgumentException("Inverse nur für 3x3 Matrix implementiert.");
        double det = m[0][0]*(m[1][1]*m[2][2]-m[2][1]*m[1][2]) - m[0][1]*(m[1][0]*m[2][2]-m[1][2]*m[2][0]) + m[0][2]*(m[1][0]*m[2][1]-m[1][1]*m[2][0]);
        if (Math.abs(det)<1e-12) return null;
        double invDet = 1.0/det;
        double[][] inv = new double[3][3];
        inv[0][0] = (m[1][1]*m[2][2]-m[2][1]*m[1][2])*invDet;
        inv[0][1] = (m[0][2]*m[2][1]-m[0][1]*m[2][2])*invDet;
        inv[0][2] = (m[0][1]*m[1][2]-m[0][2]*m[1][1])*invDet;
        inv[1][0] = (m[1][2]*m[2][0]-m[1][0]*m[2][2])*invDet;
        inv[1][1] = (m[0][0]*m[2][2]-m[0][2]*m[2][0])*invDet;
        inv[1][2] = (m[1][0]*m[0][2]-m[0][0]*m[1][2])*invDet;
        inv[2][0] = (m[1][0]*m[2][1]-m[2][0]*m[1][1])*invDet;
        inv[2][1] = (m[2][0]*m[0][1]-m[0][0]*m[2][1])*invDet;
        inv[2][2] = (m[0][0]*m[1][1]-m[1][0]*m[0][1])*invDet;
        return inv;
    }

    private double[][] computeCovarianceMatrix(double[] res1, double[] res2) {
        int n = res1.length;
        double var1=0, var2=0, cov=0;
        for (int i=0;i<n;i++){ var1+=res1[i]*res1[i]; var2+=res2[i]*res2[i]; cov+=res1[i]*res2[i];}
        double df = n-1.0; var1/=df; var2/=df; cov/=df;
        return new double[][]{{var1,cov},{cov,var2}};
    }

    private double[][] cholesky(double[][] A) {
        if (A.length!=2 || A[0].length!=2) throw new IllegalArgumentException("Cholesky nur für 2x2 Matrix");
        double[][] L = new double[2][2];
        if (A[0][0]<=0 || A[1][1]-A[1][0]*A[1][0]<=0) throw new ArithmeticException("Matrix nicht positiv definit");
        L[0][0]=Math.sqrt(A[0][0]); L[0][1]=0;
        L[1][0]=A[1][0]/L[0][0]; L[1][1]=Math.sqrt(A[1][1]-L[1][0]*L[1][0]);
        return L;
    }

    private static String getAndValidateMetricKey(Map<String, List<Double>> valuesMap, String paramName) {
        if (valuesMap == null || valuesMap.isEmpty() || valuesMap.size() != 1) {
            throw new IllegalArgumentException(paramName + " muss genau eine Serie enthalten.");
        }
        return valuesMap.keySet().iterator().next();
    }
}
