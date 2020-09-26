package com.lwwale.models;

import java.io.Serializable;

public class FunctionProperty<T> implements Serializable {
    private String propertyName;
    private String propertyDescription;

    public FunctionProperty(String propertyName, String propertyDescription) {
        this.propertyName = propertyName;
        this.propertyDescription = propertyDescription;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public FunctionProperty<T> setPropertyName(String propertyName) {
        this.propertyName = propertyName;
        return this;
    }

    public String getPropertyDescription() {
        return propertyDescription;
    }

    public FunctionProperty<T> setPropertyDescription(String propertyDescription) {
        this.propertyDescription = propertyDescription;
        return this;
    }

    public T cast(Object object) {
        return (T) object;
    }

  /*  public Class<T> getDataType() {
        return dataType;
    }

    public FunctionProperty<T> setDataType(Class<T> dataType) {
        this.dataType = dataType;
        return this;
    }*/

    @Override
    public String toString() {
        return propertyName;
    }
}
