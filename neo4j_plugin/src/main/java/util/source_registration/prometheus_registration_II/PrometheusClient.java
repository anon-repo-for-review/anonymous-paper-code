package util.source_registration.prometheus_registration_II;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;


public interface PrometheusClient {

    String[] POD_LABEL_CANDIDATES = new String[] {
            "pod","pod_name","kubernetes_pod_name","container","container_name","name",
            "container_label_io_kubernetes_pod_name","exported_pod","pod_id","com_docker_compose_service"
    };

    class LabelValuesResult {
        public final String label;
        public final List<String> values;
        public LabelValuesResult(String label, List<String> values) { this.label = label; this.values = values; }
    }

    class TimeRange {
        // Using ZonedDateTime (UTC) instead of ISO strings for consistency
        public final ZonedDateTime start;
        public final ZonedDateTime end;
        public TimeRange(ZonedDateTime start, ZonedDateTime end) { this.start = start; this.end = end; }
    }

    List<String> fetchServerNames() throws Exception;

    // NEU: Zeitspanne für Server ermitteln (ähnlich wie Pods)
    TimeRange getServerTimeRange(String serverName) throws Exception;

    // NEU: Mapping Map<PodName, ServerName>
    Map<String, String> fetchPodToServerMapping() throws Exception;

    List<String> fetchMetricNames() throws Exception;
    LabelValuesResult fetchFirstAvailableLabelValuesAndLabel() throws Exception;
    TimeRange getPodTimeRange(String podName, String genericLabel) throws Exception;
    ZonedDateTime getPodLastSeenAsZdt(String label, String podName) throws Exception;
    long getPrometheusTime() throws Exception;
}
