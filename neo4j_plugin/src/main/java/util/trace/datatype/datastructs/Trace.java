package util.trace.datatype.datastructs;

import java.util.List;
import java.util.Map;

public class Trace {
    private String traceID;
    private List<Span> spans;
    private Map<String, Process> processes;

    // Getters und Setters
    public String getTraceID() {
        return traceID;
    }
    public void setTraceID(String traceID) {
        this.traceID = traceID;
    }
    public List<Span> getSpans() {
        return spans;
    }
    public void setSpans(List<Span> spans) {
        this.spans = spans;
    }
    public Map<String, Process> getProcesses() {
        return processes;
    }
    public void setProcesses(Map<String, Process> processes) {
        this.processes = processes;
    }
}