package com.lwwale.models;

import com.lwwale.functions.DQCheckType;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Map;

public class DqCheckModel implements Serializable {
    private DQCheckType dqCheckType;
    private String columnName;
    private Map<String, Serializable> parameters;
    private int sequence;
    private String outputColumnName;

    public DqCheckModel(DQCheckType dqCheckType) {
        this.dqCheckType = dqCheckType;
    }

    public DQCheckType getDqCheckType() {
        return dqCheckType;
    }

    public String getColumnName() {
        return columnName;
    }

    public DqCheckModel setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public Map<String, Serializable> getParameters() {
        return parameters;
    }

    public DqCheckModel setParameters(Map<String, Serializable> parameters) {
        this.parameters = parameters;
        return this;
    }

    public int getSequence() {
        return sequence;
    }

    public DqCheckModel setSequence(int sequence) {
        this.sequence = sequence;
        return this;
    }

    public String getOutputColumnName() {
        return outputColumnName;
    }

    public DqCheckModel setOutputColumnName(@NotNull String outputColumnName) {
        this.outputColumnName = outputColumnName;
        return this;
    }

    @Override
    public String toString() {
        return "DqCheckModel{" +
                "sequence=" + sequence +
                ", dqCheckType=" + dqCheckType +
                ", columnName='" + columnName + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
