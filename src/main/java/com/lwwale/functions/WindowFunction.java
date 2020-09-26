package com.lwwale.functions;

import com.lwwale.models.FunctionProperty;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public abstract class WindowFunction extends Function {

    private static final String DELIMITER = ",";
    public static final FunctionProperty<String> WINDOW_COLUMNS = new FunctionProperty<>("window_columns", DELIMITER + " separated column names to group window on.");

    @Override
    protected Column apply(Column column, Map<String, Serializable> parameters) {
        return apply(Collections.unmodifiableMap(parameters), column).over(getWindowSpec(checkAndGetProperty(parameters, WINDOW_COLUMNS)));
    }

    protected WindowSpec getWindowSpec(String windowColumnsString) {
        String[] windowColumns = windowColumnsString.split(DELIMITER);
        if (windowColumns.length > 1) {
            return Window.orderBy(windowColumns[0], Arrays.copyOfRange(windowColumns, 1, windowColumns.length));
        } else {
            return Window.orderBy(windowColumns[0]);
        }
    }

    protected Column apply(Map<String, Serializable> parameters, Column column) {
        try {
            return apply(parameters);
        } catch (UnsupportedOperationException ex) {
            return apply(column);
        }
    }

    protected Column apply(Map<String, Serializable> parameters) {
        return apply();
    }

    protected Column apply(Column column) {
        return apply();
    }

    protected Column apply() {
        throw new UnsupportedOperationException("Required Column/Parameters not found");
    }
}
