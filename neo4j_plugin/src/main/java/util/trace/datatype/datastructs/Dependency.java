package util.trace.datatype.datastructs;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Dependency {
    @SerializedName("data")
    private List<DependencyData> data;

    @SerializedName("total")
    private Integer total;
    @SerializedName("limit")
    private Integer limit;
    @SerializedName("offset")
    private Integer offset;
    @SerializedName("errors")
    private Object errors;

    public List<DependencyData> getData() {
        return data;
    }

    public Dependency setData(List<DependencyData> data) {
        this.data = data;
        return this;
    }

    public Integer getTotal() {
        return total;
    }

    public Dependency setTotal(Integer total) {
        this.total = total;
        return this;
    }

    public Integer getLimit() {
        return limit;
    }

    public Dependency setLimit(Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer getOffset() {
        return offset;
    }

    public Dependency setOffset(Integer offset) {
        this.offset = offset;
        return this;
    }

    public Object getErrors() {
        return errors;
    }

    public Dependency setErrors(Object errors) {
        this.errors = errors;
        return this;
    }
}
