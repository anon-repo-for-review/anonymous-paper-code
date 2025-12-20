package util.trace.datatype.datastructs;

import com.google.gson.annotations.SerializedName;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

public class Span {
    private String traceID;
    private String spanID;
    private long flags;
    private String operationName;
    private List<Reference> references;
    private long startTime;
    private long duration;
    private List<Tag> tags;
    private List<Log> logs;
    @SerializedName("processID")
    private String processId;

    // ÄNDERUNG: Typ Object akzeptiert ALLES (String, Array oder null)
    private Object warnings;

    // --- Getter / Setter Standard ---

    public String getTraceID() {
        return traceID;
    }
    public void setTraceID(String traceID) {
        this.traceID = traceID;
    }
    public String getSpanID() {
        return spanID;
    }
    public void setSpanID(String spanID) {
        this.spanID = spanID;
    }
    public long getFlags() {
        return flags;
    }
    public void setFlags(long flags) {
        this.flags = flags;
    }
    public String getOperationName() {
        return operationName;
    }
    public void setOperationName(String operationName) {
        this.operationName = operationName;
    }
    public List<Reference> getReferences() {
        return references;
    }
    public void setReferences(List<Reference> references) {
        this.references = references;
    }
    public long getStartTime() {
        return startTime;
    }
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    public long getDuration() {
        return duration;
    }
    public void setDuration(long duration) {
        this.duration = duration;
    }
    public List<Tag> getTags() {
        return tags;
    }
    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }
    public List<Log> getLogs() {
        return logs;
    }
    public void setLogs(List<Log> logs) {
        this.logs = logs;
    }
    public String getProcessId() {
        return processId;
    }
    public void setProcessId(String processId) {
        this.processId = processId;
    }

    // --- ÄNDERUNG: Getter/Setter für das rohe Object ---

    public Object getWarnings() {
        return warnings;
    }

    public void setWarnings(Object warnings) {
        this.warnings = warnings;
    }

    // --- ZUSATZ: Robuste Hilfsmethode ---
    // Benutzen Sie diese Methode in Ihrem Code, wenn Sie die Warnungen lesen wollen.
    // Sie wandelt Einzel-Strings automatisch in eine Liste um.
    public List<String> getWarningsSafe() {
        if (warnings == null) {
            return Collections.emptyList();
        }

        if (warnings instanceof List) {
            // Es ist bereits eine Liste (Docker Szenario)
            // Wir müssen sicherstellen, dass es eine List<String> ist, Gson liefert hier meist rohe Listen
            List<?> list = (List<?>) warnings;
            List<String> result = new ArrayList<>();
            for (Object o : list) {
                if (o != null) result.add(o.toString());
            }
            return result;
        } else if (warnings instanceof String) {
            // Es ist ein einzelner String (Kubernetes Szenario)
            return Collections.singletonList((String) warnings);
        } else {
            // Fallback für unerwartete Typen (z.B. Zahlen)
            return Collections.singletonList(warnings.toString());
        }
    }
}