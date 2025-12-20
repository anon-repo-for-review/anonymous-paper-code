package util.sources;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.logging.Log;
import result_classes.TimeSeriesResult;
import util.TimeSeriesSource;
import util.TimeWindow;
import util.TimeSeriesUtil;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static util.TimeSeriesUtil.*;

public class LocalTimeSeriesSource implements TimeSeriesSource {

    @Override
    public Optional<TimeSeriesResult> fetch(Node sourceNode, Node startNode, String tsName, TimeWindow window, Log log, Map<String, Object> params) {
        if (sourceNode == null) return Optional.empty();
        if (!sourceNode.hasLabel(Label.label("time_series"))) return Optional.empty();
        String name = (String) sourceNode.getProperty("name", null);
        if (name == null || !name.equals(tsName)) return Optional.empty();

        // Parameter abfragen
        String podName = (String) params.getOrDefault("pod", "");
        String operationName = (String) params.getOrDefault("operation", "");

        // --- Neue Logik: Filter nach Pod/Operation ---
        if (!podName.isEmpty()) {
            boolean hasPod = sourceNode.getRelationships(Direction.INCOMING, RelationshipType.withName("HAS_TIME_SERIES"))
                    .stream()
                    .map(rel -> rel.getStartNode())
                    .anyMatch(n -> n.hasLabel(Label.label("Pod")) && podName.equals(n.getProperty("name", "")));

            if (!hasPod) {
                log.info("Zeitreihe '{}' wurde gefiltert, da kein Pod '{}' gefunden wurde.", tsName, podName);
                return Optional.empty();
            }
        } else if (!operationName.isEmpty()) {
            boolean hasOperation = sourceNode.getRelationships(Direction.INCOMING, RelationshipType.withName("HAS_TIME_SERIES"))
                    .stream()
                    .map(rel -> rel.getStartNode())
                    .anyMatch(n -> n.hasLabel(Label.label("Operation")) && operationName.equals(n.getProperty("name", "")));

            if (!hasOperation) {
                log.info("Zeitreihe '{}' wurde gefiltert, da keine Operation '{}' gefunden wurde.", tsName, operationName);
                return Optional.empty();
            }
        }
        // Wenn weder pod noch operation gesetzt ist, wird nicht gefiltert → alles kommt durch.

        try {
            TimeSeriesResult result = getFilteredTimeSeriesFromTsNode(sourceNode, window.startTime, window.endTime);
            return Optional.ofNullable(result);
        } catch (Exception e) {
            log.error("LocalTimeSeriesSource.fetch failed for '%s': %s", tsName, e.getMessage());
            return Optional.empty();
        }
    }

    // -------------------------------------
    // Rest deiner Hilfsmethoden unverändert
    // -------------------------------------

    public TimeSeriesResult getFilteredTimeSeriesFromTsNode(Node tsNode, long startTime, long endTime) {
        validateTimeSeries(tsNode);

        List<String> allTimestamps = Arrays.asList((String[]) tsNode.getProperty("timestamps", new String[0]));
        Map<String, List<Double>> allValues = getDoubleTs(tsNode, allTimestamps.size());

        List<Integer> indicesToKeep = new ArrayList<>();
        List<String> filteredTimestamps = new ArrayList<>();

        for (int i = 0; i < allTimestamps.size(); i++) {
            try {
                long currentTimestamp = ZonedDateTime.parse(allTimestamps.get(i)).toInstant().toEpochMilli();
                if (currentTimestamp >= startTime && currentTimestamp < endTime) {
                    indicesToKeep.add(i);
                    filteredTimestamps.add(allTimestamps.get(i));
                }
            } catch (Exception e) {
                // skip invalid timestamp
            }
        }

        Map<String, List<Double>> filteredValues = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : allValues.entrySet()) {
            List<Double> originalValues = entry.getValue();
            List<Double> newValues = indicesToKeep.stream()
                    .map(originalValues::get)
                    .map(v -> (v == null || v.isNaN() || v.isInfinite()) ? 0.0 : v)  // NaN/Inf → 0
                    .collect(Collectors.toList());
            filteredValues.put(entry.getKey(), newValues);
        }

        return new TimeSeriesResult(filteredTimestamps, filteredValues);
    }

    public void validateTimeSeries(Node tsNode) {
        if (!tsNode.hasLabel(Label.label("time_series")) || !tsNode.hasProperty("timestamps")) {
            throw new IllegalArgumentException("Knoten muss `time_series` mit `timestamps` enthalten.");
        }
    }

    public Map<String, List<Double>> getDoubleTs(Node node, int expectedLength) {
        Map<String, List<Double>> result = new HashMap<>();

        for (String key : node.getPropertyKeys()) {
            if (key.equals("timestamps") || key.equals("name")) continue;
            if (!node.hasProperty(key)) continue;

            Object raw = node.getProperty(key);
            if (!(raw instanceof Object[] || raw instanceof int[] || raw instanceof long[] || raw instanceof double[]))
                continue;

            Object[] array = convertToObjectArray(raw);
            if (array == null || array.length != expectedLength) continue;

            List<Double> doubleList = new ArrayList<>();
            for (Object obj : array) {
                if (obj instanceof Number) doubleList.add(((Number) obj).doubleValue());
            }
            result.put(key, doubleList);
        }
        return result;
    }

    private Object[] convertToObjectArray(Object raw) {
        if (raw instanceof Object[]) return (Object[]) raw;
        if (raw instanceof int[]) return Arrays.stream((int[]) raw).boxed().toArray();
        if (raw instanceof long[]) return Arrays.stream((long[]) raw).boxed().toArray();
        if (raw instanceof double[]) return Arrays.stream((double[]) raw).boxed().toArray();
        return null;
    }
}
