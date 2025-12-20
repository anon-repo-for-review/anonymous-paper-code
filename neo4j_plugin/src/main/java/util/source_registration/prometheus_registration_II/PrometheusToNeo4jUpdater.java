package util.source_registration.prometheus_registration_II;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;

public final class PrometheusToNeo4jUpdater {
    private static final ConcurrentMap<String, ScheduledExecutorService> SCHED = new ConcurrentHashMap<>();
    private final PrometheusClient client;
    private final PodRepository repo;
    private final Log log;
    private final String prometheusUrl;
    private final long intervalSeconds;
    private final long GRACE_PERIOD_SECONDS = 60 * 5;
    private final GraphDatabaseService db;

    public PrometheusToNeo4jUpdater(PrometheusClient client, PodRepository repo, Log log, String url, long intervalSeconds, GraphDatabaseService db) {
        this.client = client; this.repo = repo; this.log = log; this.prometheusUrl = url; this.intervalSeconds = intervalSeconds; this.db = db;    }

    public void startOrEnsureUpdater() {
        SCHED.computeIfAbsent(prometheusUrl, u -> {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("prometheus-updater-" + Math.abs(u.hashCode()));
                return t;
            });
            Runnable task = () -> {
                try { synchronizeOnce(); }
                catch (Exception e) { log.error("Updater error for {}: {}", prometheusUrl, e.getMessage(), e); }
            };
            scheduler.scheduleAtFixedRate(task, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
            return scheduler;
        });
    }

    private void synchronizeOnce() throws Exception {
        PrometheusClient.LabelValuesResult labelRes = client.fetchFirstAvailableLabelValuesAndLabel();
        if (labelRes == null || labelRes.values == null || labelRes.values.isEmpty()) {
            log.info("No pods found in Prometheus: {}", prometheusUrl);
            return;
        }
        // 1) fetch last-seen per pod (prometheus side)
        Map<String,ZonedDateTime> promMap = new HashMap<>();
        Instant now = Instant.now();
        for (String pod : labelRes.values) {
            try {
                ZonedDateTime ts = client.getPodLastSeenAsZdt(labelRes.label, pod);
                promMap.put(pod, ts);
            } catch (Exception e) {
                log.warn("Could not fetch lastSeen for {}: {}", pod, e.getMessage());
                promMap.put(pod, ZonedDateTime.ofInstant(now, ZoneOffset.UTC));
            }
        }

        log.info("Prom: " + promMap);

        // 2) read DB state
        Map<String,ZonedDateTime> dbMap = repo.findPodEndsForPrometheus(prometheusUrl);
        log.info(" DB: " + dbMap);

        // 3) reconcile -> build bulk upserts and stops
        List<Map<String,Object>> toCreateOrUpdate = new ArrayList<>();
        List<String> toStop = new ArrayList<>();
        long promNow = client.getPrometheusTime();
        Instant graceThreshold = Instant.ofEpochSecond(promNow).minusSeconds(GRACE_PERIOD_SECONDS);

        for (Map.Entry<String,ZonedDateTime> e : promMap.entrySet()) {
            String name = e.getKey();
            ZonedDateTime promEnd = e.getValue();
            ZonedDateTime dbEnd = dbMap.get(name);
            /*Map<String,Object> row = new HashMap<>();
            row.put("name", name);
            row.put("end", promEnd);
            // if not exists -> create with start=end
            if (dbEnd == null) {
                row.put("start", promEnd);
                row.put("status", "RUNNING");
                toCreateOrUpdate.add(row);
            } else if (promEnd.isAfter(dbEnd)) {
                row.put("start", null); // no change
                row.put("status", "RUNNING");
                toCreateOrUpdate.add(row);
            }*/

            if (dbEnd == null) {
                final Map<String,Object> row = new HashMap<>();
                row.put("name", name);
                row.put("start", promEnd); // vorhanden -> initial start
                row.put("end", promEnd);
                row.put("status", "RUNNING");
                toCreateOrUpdate.add(row);
            } else if (promEnd.isAfter(dbEnd)) {
                final Map<String,Object> row = new HashMap<>();
                row.put("name", name);
                // nur end & status setzen — kein start-Key
                row.put("end", promEnd);
                row.put("status", "RUNNING");
                toCreateOrUpdate.add(row);
            }
        }

        for (Map.Entry<String,ZonedDateTime> e : dbMap.entrySet()) {
            String name = e.getKey();
            ZonedDateTime dbEnd = e.getValue();
            if (!promMap.containsKey(name) && dbEnd != null && dbEnd.toInstant().isBefore(graceThreshold)) {
                toStop.add(name);
            }
        }

        // 4) apply in DB (bulk)
        if (!toCreateOrUpdate.isEmpty()) repo.upsertPodsBulk(toCreateOrUpdate, prometheusUrl);
        if (!toStop.isEmpty()) repo.markPodsStopped(toStop, prometheusUrl);

        if (!toCreateOrUpdate.isEmpty()) {
            // Instanzieren und ausführen
            // Hinweis: Da synchronizeOnce oft läuft, ist die Link-Query effizient genug,
            // da sie nur nicht-existierende Relationen merged.
            new ServicePodLinker(db, log).linkServicesAndPods();
        }





        // --- 2. SERVER SYNCHRONIZATION (Neu) --- NOCH NICHT GETESTET !!!!!!!!!
        List<String> serverNames = client.fetchServerNames();

        // a) Fetch last-seen from Prometheus
        Map<String, ZonedDateTime> promServerMap = new HashMap<>();
        now = Instant.now();
        for (String srv : serverNames) {
            try {
                // Wir nutzen einfach getServerTimeRange().end oder eine spezifische lastSeen Methode
                PrometheusClient.TimeRange range = client.getServerTimeRange(srv);
                promServerMap.put(srv, range.end);
            } catch (Exception e) {
                promServerMap.put(srv, ZonedDateTime.ofInstant(now, ZoneOffset.UTC));
            }
        }

        // b) Read DB State for Servers
        Map<String, ZonedDateTime> dbServerMap = repo.findServerEndsForPrometheus(prometheusUrl);

        // c) Reconcile Servers
        List<Map<String, Object>> serversToUpsert = new ArrayList<>();
        List<String> serversToStop = new ArrayList<>();
        promNow = client.getPrometheusTime();


        for (Map.Entry<String, ZonedDateTime> e : promServerMap.entrySet()) {
            String name = e.getKey();
            ZonedDateTime promEnd = e.getValue();
            ZonedDateTime dbEnd = dbServerMap.get(name);

            if (dbEnd == null) {
                // Neuer Server
                Map<String, Object> row = new HashMap<>();
                row.put("name", name);
                row.put("start", promEnd);
                row.put("end", promEnd);
                row.put("status", "RUNNING");
                serversToUpsert.add(row);
            } else if (promEnd != null && promEnd.isAfter(dbEnd)) {
                // Update Server Endzeit
                Map<String, Object> row = new HashMap<>();
                row.put("name", name);
                row.put("end", promEnd);
                row.put("status", "RUNNING");
                serversToUpsert.add(row);
            }
        }

        for (Map.Entry<String, ZonedDateTime> e : dbServerMap.entrySet()) {
            String name = e.getKey();
            ZonedDateTime dbEnd = e.getValue();
            // Wenn Server in Prometheus nicht mehr da ist und Grace Period abgelaufen
            if (!promServerMap.containsKey(name) && dbEnd != null && dbEnd.toInstant().isBefore(graceThreshold)) {
                serversToStop.add(name);
            }
        }

        // d) Apply DB
        if (!serversToUpsert.isEmpty()) repo.upsertServersBulk(serversToUpsert, prometheusUrl);
        if (!serversToStop.isEmpty()) repo.markServersStopped(serversToStop, prometheusUrl);


    }
}
