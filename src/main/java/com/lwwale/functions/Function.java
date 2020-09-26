package com.lwwale.functions;

import com.lwwale.exception.MandatoryPropertyMissingException;
import com.lwwale.models.FunctionProperty;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Map;

public abstract class Function implements Serializable {
    public static <T> T checkAndGetProperty(Map<String, Serializable> parameters, FunctionProperty<T> functionProperty) {
        if (parameters != null && parameters.containsKey(functionProperty.getPropertyName())) {
            try {
                return (T) parameters.get(functionProperty.getPropertyName());
            } catch (ClassCastException ex) {
                throw new RuntimeException("Unable to bind property value to expected type [" + functionProperty.getPropertyName() + "]", ex);
            }
        }
        throw new MandatoryPropertyMissingException(functionProperty);
    }

    public abstract String getColumnPrefix();

    protected Column apply(Column column, Map<String, Serializable> parameters) {
        return apply(parameters);
    }

    protected Column apply(Column column) {
        return apply();
    }


    protected Column apply(Map<String, Serializable> parameters) {
        return apply();
    }

    protected Column apply() {
        throw new UnsupportedOperationException("Required Parameters not found [" + this.getColumnPrefix() + "]");
    }

    public Dataset<Row> evaluate(Dataset<Row> dataset, String outputColumnName, Map<String, Serializable> parameters) {
        Column outputColumn = apply(parameters).as(outputColumnName);
        return dataset.withColumn(outputColumnName, outputColumn);
    }

    public Dataset<Row> evaluate(Dataset<Row> dataset, String outputColumnName) {
        Column outputColumn = apply().as(outputColumnName);
        return dataset.withColumn(outputColumnName, outputColumn);
    }

    public Dataset<Row> evaluate(Dataset<Row> dataset, String columnName, String outputColumnName, Map<String, Serializable> parameters) {
        Column outputColumn = apply(dataset.col(columnName), parameters).as(outputColumnName);
        return dataset.withColumn(outputColumnName, outputColumn);
    }
}
