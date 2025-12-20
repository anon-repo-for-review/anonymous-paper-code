package util.source_registration.prometheus_registration_II;

import org.neo4j.logging.Log;
import java.time.Instant;
import java.util.*;

public class PrometheusRegistrationService {
    private final PrometheusClient client;
    private final PodRepository repo;
    private final Log log;

    public PrometheusRegistrationService(PrometheusClient client, PodRepository repo, Log log) {
        this.client = client;
        this.repo = repo;
        this.log = log;
    }

    public long register(String prometheusUrl) throws Exception {
        // 1) metrics
        List<String> metricNames = client.fetchMetricNames();

        // 2) ensure prometheus node
        repo.ensurePrometheusNode(prometheusUrl, metricNames);

        // 3) discover pods
        PrometheusClient.LabelValuesResult labelRes = client.fetchFirstAvailableLabelValuesAndLabel();
        List<String> podNames = labelRes.values;

        // 4) build upsert rows for pods (bulk)
        List<Map<String,Object>> rows = new ArrayList<>(podNames.size());
        for (String p : podNames) {
            PrometheusClient.TimeRange range = client.getPodTimeRange(p, labelRes.label);
            Map<String,Object> m = new HashMap<>();
            m.put("name", p);
            m.put("start", range.start);
            m.put("end", range.end);
            m.put("status", "RUNNING");
            rows.add(m);
        }

        // 5) write pods (bulk) and link to prometheus
        repo.upsertPodsBulk(rows, prometheusUrl);
        repo.linkPodsToPrometheus(prometheusUrl);


        // --- B) SERVERS (NEU) ---
        List<String> serverNames = client.fetchServerNames();
        List<Map<String, Object>> serverRows = new ArrayList<>(serverNames.size());
        for (String s : serverNames) {
            PrometheusClient.TimeRange range = client.getServerTimeRange(s); // Benutzt neue Client Methode
            Map<String, Object> m = new HashMap<>();
            m.put("name", s);
            m.put("start", range.start);
            m.put("end", range.end);
            m.put("status", "RUNNING");
            serverRows.add(m);
        }
        repo.upsertServersBulk(serverRows, prometheusUrl);

        // --- C) LINKING (NEU) ---
        // Pods mit Servern verknüpfen
        Map<String, String> podToServer = client.fetchPodToServerMapping();
        repo.linkPodsToServers(podToServer);

        // 6) optionally start updater (caller handles scheduling) — return pod count
        return podNames.size();
    }
}
