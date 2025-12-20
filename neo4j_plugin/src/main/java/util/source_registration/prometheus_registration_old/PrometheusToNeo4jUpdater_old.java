package util.source_registration.prometheus_registration_old;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;

import static util.source_registration.prometheus_registration_old.PrometheusClient_old.*;

public final class PrometheusToNeo4jUpdater_old {




// Annahmen: Klassen wie LabelValuesResult, db (Datenbankverbindung) und POD_LABEL_CANDIDATES sind vorhanden.
// Die Methoden fetchFirstAvailableLabelValuesAndLabel und getPodLastSeen sind ebenfalls vorhanden.

    private static final Map<String, ScheduledExecutorService> SCHEDULERS = new ConcurrentHashMap<>();
    private static final long GRACE_PERIOD_SECONDS = 60 * 5; //// 5 Minuten


    private final GraphDatabaseService db;
    private final Log log;
    private  final String prometheus_url;
    final long intervalSeconds;



    public PrometheusToNeo4jUpdater_old(GraphDatabaseService db, Log log, String url, long intervalSeconds) {
        this.db = db;
        this.log = log;
        this.prometheus_url = url;
        this.intervalSeconds = intervalSeconds;
    }

    /**
     * Startet oder stellt sicher, dass für eine gegebene Prometheus-URL ein Updater-Task läuft.
     * Dieser Task synchronisiert periodisch den Status von Pods zwischen Prometheus und einer Neo4j-Datenbank.
     */
    public void startOrEnsureUpdater() {
        SCHEDULERS.computeIfAbsent(prometheus_url, url -> {
            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                final Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("prometheus-updater-" + Math.abs(url.hashCode()));
                return t;
            });

            final Runnable task = () -> {
                try {
                    // Hauptlogik in eine eigene Methode ausgelagert
                    synchronizePodsFor();
                } catch (Exception e) {
                    // Ein zentraler Fehler-Handler, um den Scheduler am Leben zu erhalten
                    log.error("Unerwarteter Fehler im Prometheus-Updater für {}: {}", url, e.getMessage(), e);
                }
            };

            // Task sofort starten und dann alle 30 Sekunden wiederholen
            scheduler.scheduleAtFixedRate(task, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
            return scheduler;
        });
    }

    /**
     * Führt einen einzelnen Synchronisationslauf für eine gegebene Prometheus-URL durch.
     */
    private void synchronizePodsFor() {
        // 1. Pods und ihren letzten Zeitstempel von Prometheus abrufen
        final Map<String, ZonedDateTime> prometheusPods = discoverAndFetchPods();

        if (prometheusPods.isEmpty()) {
            log.info("Keine Pods in Prometheus für {} gefunden. Überspringe Synchronisation.", prometheus_url);
            return;
        }

        // 2. Aktuellen Zustand der Pods aus der Neo4 Datenbank laden
        final Map<String, ZonedDateTime> dbPodsEndTimes = getPodsFromDatabase();

        // 3. Zustände vergleichen, um zu erstellende, zu aktualisierende und zu stoppende Pods zu ermitteln
        final ReconciliationResult result = reconcilePodStates(prometheusPods, dbPodsEndTimes);

        // 4. Die ermittelten Änderungen auf die Datenbank anwenden
        applyDatabaseChanges(result);
    }

    /**
     * Entdeckt Pods in Prometheus und ruft für jeden Pod den zuletzt gesehenen Zeitstempel ab.
     * @return Eine Map von Pod-Namen zu ihrem letzten "gesehen" Zeitstempel (Instant).
     */
    private Map<String, ZonedDateTime> discoverAndFetchPods() {
        final Instant now = Instant.now();
        final Map<String, ZonedDateTime> podToEndTimes = new HashMap<>();

        try {
            final PrometheusClient_old.LabelValuesResult labelRes = fetchFirstAvailableLabelValuesAndLabel(prometheus_url);
            final String podLabel = labelRes.label;
            final List<String> promPods = labelRes.values != null ? labelRes.values : Collections.emptyList();

            if (podLabel == null || promPods.isEmpty()) {
                return Collections.emptyMap();
            }

            for (final String pod : promPods) {
                try {
                    final Double lastSeenSec = getPodLastSeen(prometheus_url, podLabel, pod);
                    final Instant lastSeenInstant = (lastSeenSec != null)
                            ? Instant.ofEpochSecond(lastSeenSec.longValue())
                            : now; // Fallback auf 'now', wenn keine Metrik gefunden wird
                    podToEndTimes.put(pod, ZonedDateTime.ofInstant(lastSeenInstant, ZoneOffset.UTC));
                } catch (Exception e) {
                    log.warn("Konnte lastSeen für Pod {} nicht abrufen. Verwende Fallback 'now'. Fehler: {}", pod, e.getMessage());
                    podToEndTimes.put(pod, ZonedDateTime.ofInstant(now, ZoneOffset.UTC)); // Robustheit: Bei Fehler 'now' annehmen
                }
            }
        } catch (Exception e) {
            log.error("Fehler bei der Ermittlung von Pods von Prometheus {}: {}", prometheus_url, e.getMessage());
            return Collections.emptyMap(); // Bei schwerwiegendem Fehler leere Map zurückgeben
        }
        return podToEndTimes;
    }

    /**
     * Ruft die in der Datenbank gespeicherten Pods für eine bestimmte Prometheus-Instanz ab.
     * @return Eine Map von Pod-Namen zu ihrem 'end'-Zeitstempel.
     */
    private Map<String, ZonedDateTime> getPodsFromDatabase() {
        final Map<String, ZonedDateTime> dbPods = new HashMap<>();
        final String query = "MATCH (p:Pod:time_period)-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) RETURN p.name AS name, p.end AS end";
        final Map<String, Object> params = Collections.singletonMap("url", prometheus_url);



        try (Transaction tx = db.beginTx()) {
            final Result res = tx.execute(query, params);

            while (res.hasNext()) {
                final Map<String, Object> row = res.next();
                final String name = (String) row.get("name");
                final Object endValue = row.get("end");


                if (name != null && endValue instanceof String) {
                    try {
                        dbPods.put(name, ZonedDateTime.parse(endValue.toString()));
                    } catch (Exception e) {
                        log.info("Ungültiges 'end'-Datumsformat");

                    }
                } else if (name != null && endValue instanceof ZonedDateTime) {
                    try {
                        dbPods.put(name, ((ZonedDateTime) endValue));
                    } catch (Exception e) {
                        log.info("Ungültiges 'end'-Datumsformat");

                    }
                } else if (name != null && endValue != null) {
                    try {
                        // Convert the object to a string and then parse it.
                        // This works whether the underlying type is a String, a Neo4j DateTime, etc.
                        dbPods.put(name, ZonedDateTime.parse(endValue.toString()));
                    } catch (Exception e) {
                        log.info("Ungültiges 'end'-Datumsformat for pod " + name + ": " + endValue);

                    }
                }
            }
            tx.commit();
        }

        return dbPods;
    }

    /**
     * Vergleicht die Pod-Zustände von Prometheus und der Datenbank.
     * @param prometheusPods Pods, wie sie in Prometheus existieren.
     * @param dbPodsEndTimes Pods, wie sie in der Datenbank existieren.
     * @return Ein ReconciliationResult-Objekt mit Listen für zu erstellende, zu aktualisierende und zu stoppende Pods.
     */
    private ReconciliationResult reconcilePodStates(final Map<String, ZonedDateTime> prometheusPods, final Map<String, ZonedDateTime> dbPodsEndTimes) {
        final List<Map<String, Object>> podsToCreate = new ArrayList<>();
        final List<Map<String, Object>> podsToUpdate = new ArrayList<>();
        final List<String> podsToStop = new ArrayList<>();
        final Instant graceThreshold; // Instant.now().minusSeconds(GRACE_PERIOD_SECONDS); //änden!!!
        try {
            // prüfen!!!
            graceThreshold = Instant.ofEpochSecond(PrometheusClient_old.getPrometheusTime(prometheus_url)).minusSeconds(GRACE_PERIOD_SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }



        // Pods aus Prometheus durchgehen: Erstellen oder aktualisieren?
        for (final Map.Entry<String, ZonedDateTime> entry : prometheusPods.entrySet()) {
            final String podName = entry.getKey();
            final ZonedDateTime promEndTime = entry.getValue();

            if (!dbPodsEndTimes.containsKey(podName)) {
                // Neuer Pod
                final Map<String, Object> podData = new HashMap<>();
                podData.put("name", podName);
                podData.put("start", promEndTime);
                podData.put("end", promEndTime);
                podsToCreate.add(podData);
            } else {
                // Existierender Pod: Nur aktualisieren, wenn der neue Zeitstempel später ist
                final ZonedDateTime dbEndTime = dbPodsEndTimes.get(podName);
                if (promEndTime.isAfter(dbEndTime)) {
                    final Map<String, Object> podData = new HashMap<>();
                    podData.put("name", podName);
                    podData.put("end", promEndTime);
                    podsToUpdate.add(podData);
                }
            }
        }



        // Pods aus der DB durchgehen: Stoppen?
        for (final Map.Entry<String, ZonedDateTime> entry : dbPodsEndTimes.entrySet()) {
            final String dbPodName = entry.getKey();
            final ZonedDateTime dbEndTime = entry.getValue();

            if (!prometheusPods.containsKey(dbPodName) && dbEndTime.isBefore(ZonedDateTime.ofInstant(graceThreshold, ZoneOffset.UTC))) {
                // Pod ist nicht mehr in Prometheus und die Grace Period ist abgelaufen
                podsToStop.add(dbPodName);
            }
        }

        return new ReconciliationResult(podsToCreate, podsToUpdate, podsToStop);
    }

    /**
     * Wendet die Änderungen (Erstellen, Aktualisieren, Stoppen) in der Neo4j-Datenbank an.
     * @param result Das Ergebnis des Abgleichs.
     */
    private void applyDatabaseChanges(final ReconciliationResult result) {
        // 5) Neue Pods in der DB erstellen
        if (!result.podsToCreate.isEmpty()) {
            final String createQuery = "UNWIND $rows AS r MERGE (p:Pod:time_period {name:r.name}) " +
                    "SET p.start = r.start, p.end = r.end, p.status = 'RUNNING' " +
                    "WITH p MATCH (pr:Prometheus {url:$url}) MERGE (p)-[:HAS_TIME_SERIES]->(pr)";
            executeDbUpdate("Erstellen", createQuery, Collections.singletonMap("rows", result.podsToCreate));
        }

        // 6) Bestehende Pods aktualisieren
        if (!result.podsToUpdate.isEmpty()) {
            final String updateQuery = "UNWIND $rows AS r MATCH (p:Pod:time_period {name:r.name})-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) " +
                    "SET p.end = r.end, p.status = 'RUNNING'";
            executeDbUpdate("Aktualisieren", updateQuery, Collections.singletonMap("rows", result.podsToUpdate));
        }

        // 7) Abwesende Pods als 'STOPPED' markieren
        if (!result.podsToStop.isEmpty()) {
            final String stopQuery = "UNWIND $names AS n MATCH (p:Pod:time_period {name:n})-[:HAS_TIME_SERIES]->(pr:Prometheus {url:$url}) " +
                    "SET p.status = 'STOPPED'";
            executeDbUpdate("Stoppen", stopQuery, Collections.singletonMap("names", result.podsToStop));
        }
    }

    /**
     * Hilfsmethode zur Ausführung von Schreib-Transaktionen in der Datenbank.
     */
    private void executeDbUpdate(final String action, final String query, final Map<String, Object> data) {
        try (Transaction tx = db.beginTx()) {
            final Map<String, Object> params = new HashMap<>(data);
            params.put("url", prometheus_url);
            tx.execute(query, params);
            tx.commit();
        } catch (Exception e) {
            log.error("Fehler beim {} von Pods für {}: {}", action, prometheus_url, e.getMessage(), e);
        }
    }

    /**
     * Eine einfache Container-Klasse für die Ergebnisse des Abgleichs.
     */
    private static class ReconciliationResult {
        final List<Map<String, Object>> podsToCreate;
        final List<Map<String, Object>> podsToUpdate;
        final List<String> podsToStop;

        ReconciliationResult(List<Map<String, Object>> podsToCreate, List<Map<String, Object>> podsToUpdate, List<String> podsToStop) {
            this.podsToCreate = podsToCreate;
            this.podsToUpdate = podsToUpdate;
            this.podsToStop = podsToStop;
        }
    }


}
