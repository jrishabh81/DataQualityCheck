package com.lwwale.functions.impl;

import com.lwwale.functions.Function;
import com.lwwale.models.FunctionProperty;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;


public class ComparisionFunctions {
    private ComparisionFunctions() {
    }

    public static class Abs extends Function {
        @Override
        public String getColumnPrefix() {
            return "_abs";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.abs(column);
        }
    }

    public static class NullCheckFunction extends Function {
        @Override
        public String getColumnPrefix() {
            return "_is_null";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return column.isNull();
        }
    }

    public static class NotNullCheckFunction extends Function {
        @Override
        public String getColumnPrefix() {
            return "_is_not_null";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.isNotNull();
        }
    }

    public static class IsNANFunction extends Function {
        @Override
        public String getColumnPrefix() {
            return "_is_NAN";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.isNaN();
        }
    }

    public static class IsEqualToFunction extends Function {
        public static final FunctionProperty<?> VALUE = new FunctionProperty<>("value", "Value to which column value has to be compared with.");

        @Override
        public String getColumnPrefix() {
            return "_is_equal_to";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.eqNullSafe(checkAndGetProperty(parameters, VALUE));
        }
    }

    public static class IsNotEqualToFunction extends Function {
        public static final FunctionProperty<?> VALUE = new FunctionProperty<>("value", "Value to which column value has to be compared with.");

        @Override
        public String getColumnPrefix() {
            return "_is_not_equal_to";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.notEqual(checkAndGetProperty(parameters, VALUE));
        }
    }

    public static class LessThanFunction extends Function {
        public static final FunctionProperty<?> VALUE = new FunctionProperty<>("value", "Value to which column value has to be compared with.");

        @Override
        public String getColumnPrefix() {
            return "_is_lt";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.lt(checkAndGetProperty(parameters, VALUE));
        }
    }

    public static class LessThanEqualToFunction extends Function {
        public static final FunctionProperty<?> VALUE = new FunctionProperty<>("value", "Value to which column value has to be compared with.");

        @Override
        public String getColumnPrefix() {
            return "_is_leq";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.leq(checkAndGetProperty(parameters, VALUE));
        }
    }

    public static class GreaterThanFunction extends Function {
        public static final FunctionProperty<?> VALUE = new FunctionProperty<>("value", "Value to which column value has to be compared with.");

        @Override
        public String getColumnPrefix() {
            return "_is_gt";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.gt(checkAndGetProperty(parameters, VALUE));
        }
    }

    public static class GreaterThanEqualToFunction extends Function {
        public static final FunctionProperty<?> VALUE = new FunctionProperty<>("value", "Value to which column value has to be compared with.");

        @Override
        public String getColumnPrefix() {
            return "_is_geq";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.geq(checkAndGetProperty(parameters, VALUE));
        }
    }

    public static class BetweenFunction extends Function {
        public static final FunctionProperty<?> L_VALUE = new FunctionProperty<>("l_value", "Lower Value to which column value has to be compared with.");
        public static final FunctionProperty<?> U_VALUE = new FunctionProperty<>("u_value", "Upper Value to which column value has to be compared with.");

        @Override
        public String getColumnPrefix() {
            return "_is_between";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.between(checkAndGetProperty(parameters, L_VALUE), checkAndGetProperty(parameters, U_VALUE));
        }
    }

    public static class InFunction extends Function {
        public static final FunctionProperty<Set<?>> VALUE = new FunctionProperty<>("value", "java.util.collection.Set of values to which column value has to be matched in.");

        @Override
        public String getColumnPrefix() {
            return "_is_in";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.isin(checkAndGetProperty(parameters, VALUE));
        }
    }

    public static class LikeFunction extends Function {
        public static final FunctionProperty<String> REGEX = new FunctionProperty<>("regex", "Regular expression.");

        @Override
        public String getColumnPrefix() {
            return "_like";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.rlike(checkAndGetProperty(parameters, REGEX));
        }
    }

    public static class SQLLikeFunction extends Function {
        public static final FunctionProperty<String> SQL_REGEX = new FunctionProperty<>("regex", "SQL Compliant regular expression.");

        @Override
        public String getColumnPrefix() {
            return "_sql_like";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return column.like(checkAndGetProperty(parameters, SQL_REGEX));
        }
    }

    public static class Test extends Function {
        @Override
        public String getColumnPrefix() {
            return "_test";
        }

        @Override
        protected Column apply(Column column, Map<String, Serializable> parameters) {
            return functions.mean(column);
        }
    }
}
