package result_classes;

import java.util.List;
import java.util.Map;

public class TimeSeriesResult {
    public List<String> timestamps;
    public Map<String, List<Double>> values;

    public TimeSeriesResult(List<String> timestamps, Map<String, List<Double>> values) {
        this.timestamps = timestamps;
        this.values = values;
    }

}
