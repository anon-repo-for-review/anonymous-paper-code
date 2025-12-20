package util.trace.datatype.datastructs;

import com.google.gson.annotations.SerializedName;

public class DependencyData {
    @SerializedName("parent")
    private String parent;
    @SerializedName("child")
    private String child;
    @SerializedName("callCount")
    private int callCount;

    public String getParent() {
        return parent;
    }

    public DependencyData setParent(String parent) {
        this.parent = parent;
        return this;
    }

    public String getChild() {
        return child;
    }

    public DependencyData setChild(String child) {
        this.child = child;
        return this;
    }

    public int getCallCount() {
        return callCount;
    }

    public DependencyData setCallCount(int callCount) {
        this.callCount = callCount;
        return this;
    }
}
