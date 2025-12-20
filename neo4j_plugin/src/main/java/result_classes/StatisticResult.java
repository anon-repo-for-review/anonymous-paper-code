package result_classes;

import java.util.List;
import java.util.Map;

public class StatisticResult {
    //List<String> properties;
    public Map<String, Double> values;

    public StatisticResult(Map<String, Double> values) {
        //this.properties = properties;
        this.values = values;
    }
}