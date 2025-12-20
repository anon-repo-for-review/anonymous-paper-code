package util.trace.datatype.datastructs;

import com.google.gson.annotations.SerializedName;

public class Reference {
    @SerializedName("refType")
    private String refType;
    private String traceID;
    private String spanID;

    public String getRefType() {
        return refType;
    }
    public void setRefType(String refType) {
        this.refType = refType;
    }
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
}