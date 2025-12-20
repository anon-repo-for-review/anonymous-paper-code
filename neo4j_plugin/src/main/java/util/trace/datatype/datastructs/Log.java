package util.trace.datatype.datastructs;

import java.util.List;

public class Log {
    private long timestamp;
    private List<Tag> fields;

    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public List<Tag> getFields() {
        return fields;
    }
    public void setFields(List<Tag> fields) {
        this.fields = fields;
    }
}