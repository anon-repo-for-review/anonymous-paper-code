package util.source_registration.prometheus_registration_II;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

public interface PodRepository {
    long ensurePrometheusNode(String url, List<String> metricNames);
    void createOrUpdateServers(List<Map<String,Object>> servers); // each map: name,start,end
    /**
     * Bulk upsert von Pods.
     * rows: List of Map with keys: name, start (ISO), end (ISO), status (optional)
     */
    void upsertPodsBulk(List<Map<String,Object>> rows, String prometheusUrl);
    /**
     * Liefert p.name -> p.end (als ISO parsebar) für alle Pods, die an die Prometheus node gebunden sind.
     */
    java.util.Map<String, java.time.ZonedDateTime> findPodEndsForPrometheus(String prometheusUrl);
    void markPodsStopped(List<String> podNames, String prometheusUrl);
    void linkPodsToPrometheus(String prometheusUrl);

    void upsertServersBulk(List<Map<String, Object>> rows, String prometheusUrl);
    void linkPodsToServers(Map<String, String> podToServerMap);
    Map<String, ZonedDateTime> findServerEndsForPrometheus(String prometheusUrl); // Für den Updater
    void markServersStopped(List<String> names, String prometheusUrl);
}
