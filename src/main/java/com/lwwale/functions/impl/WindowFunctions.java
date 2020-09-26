package com.lwwale.functions.impl;

import com.lwwale.functions.WindowFunction;
import com.lwwale.models.FunctionProperty;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.util.Map;

import static com.lwwale.functions.impl.WindowFunctions.CorrelationFunction.COLUMN_NAME;

public class WindowFunctions {
    private WindowFunctions() {
    }

    public static class Average extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_avg";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.avg(column);
        }
    }

    public static class Kurtosis extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_kurtosis";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.kurtosis(column);
        }
    }

    public static class Max extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_max";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.max(column);
        }
    }

    public static class Min extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_min";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.min(column);
        }
    }

    public static class Mean extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_mean";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.mean(column);
        }
    }

    public static class Skewness extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_skewness";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.skewness(column);
        }
    }

    public static class StdDev extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_std_dev";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.stddev(column);
        }
    }

    public static class Sum extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_sum";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.sum(column);
        }
    }

    public static class Variance extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_variance";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.variance(column);
        }
    }

    public static class LagFunction extends WindowFunction {
        public static final FunctionProperty<Serializable> DEFAULT_VALUE = new FunctionProperty<>("default_value", "if there is less than `offset` rows before the current row.");
        public static final FunctionProperty<Integer> OFFSET = new FunctionProperty<>("offset", "Size of the step to lag");

        @Override
        public String getColumnPrefix() {
            return "_lag";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            int offset = checkAndGetProperty(parameters, OFFSET);
            Object defaultValue = checkAndGetProperty(parameters, DEFAULT_VALUE);
            return functions.lag(column, offset, defaultValue);
        }
    }

    public static class LeadFunction extends WindowFunction {
        public static final FunctionProperty<Serializable> DEFAULT_VALUE = new FunctionProperty<>("default_value", "if there is less than `offset` rows after the current row.");
        public static final FunctionProperty<Integer> OFFSET = new FunctionProperty<>("offset", "Size of the step to lead");

        @Override
        public String getColumnPrefix() {
            return "_lead";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            int offset = checkAndGetProperty(parameters, OFFSET);
            Object defaultValue = checkAndGetProperty(parameters, DEFAULT_VALUE);
            return functions.lead(column, offset, defaultValue);
        }
    }

    public static class MovingAverageFunction extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_mavg";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return this.apply(column);
        }

        @Override
        protected Column apply(Column column) {
            return functions.avg(column);
        }
    }

    /**
     * returns the Pearson Correlation Coefficient for two columns.
     */
    public static class CorrelationFunction extends WindowFunction {
        public static final FunctionProperty<String> COLUMN_NAME = new FunctionProperty<>("columnName", "Column Name to find correlation with.");

        @Override
        public String getColumnPrefix() {
            return "_corr";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return functions.corr(column.toString(), checkAndGetProperty(parameters, COLUMN_NAME));
        }
    }

    public static class PopulationCovariance extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_covar_pop";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return functions.covar_pop(column.toString(), checkAndGetProperty(parameters, COLUMN_NAME));
        }
    }

    public static class SampleCovariance extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_covar_samp";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters, Column column) {
            return functions.covar_samp(column.toString(), checkAndGetProperty(parameters, COLUMN_NAME));
        }
    }

    /* Functions without column */

    /**
     * Window function: returns the rank of rows within a window partition, without any gaps.
     * <p>
     * The difference between rank and dense_rank is that denseRank leaves no gaps in ranking
     * sequence when there are ties. That is, if you were ranking a competition using dense_rank
     * and had three people tie for second place, you would say that all three were in second
     * place and that the next person came in third. Rank would give me sequential numbers, making
     * the person that came in third place (after the ties) would register as coming in fifth.
     * <p>
     * This is equivalent to the DENSE_RANK function in SQL.
     */
    public static class DenseRank extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_dense_rank";
        }

        @Override
        protected Column apply() {
            return functions.dense_rank();
        }
    }

    /**
     * Window function: returns the relative rank (i.e. percentile) of rows within a window partition.
     * <p>
     * This is computed by:
     * {{{
     * (rank of row in its partition - 1) / (number of rows in the partition - 1)
     * }}}
     * <p>
     * This is equivalent to the PERCENT_RANK function in SQL.
     */
    public static class PercentRank extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_percent_rank";
        }

        @Override
        protected Column apply() {
            return functions.percent_rank();
        }
    }

    /**
     * The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
     * sequence when there are ties. That is, if you were ranking a competition using dense_rank
     * and had three people tie for second place, you would say that all three were in second
     * place and that the next person came in third. Rank would give me sequential numbers, making
     * the person that came in third place (after the ties) would register as coming in fifth.
     * <p>
     * This is equivalent to the RANK function in SQL.
     **/
    public static class Rank extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_rank";
        }

        @Override
        protected Column apply() {
            return functions.rank();
        }
    }

    public static class CumulativeDistribution extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_cume_dist";
        }

        @Override
        protected Column apply() {
            return functions.cume_dist();
        }
    }

    public static class NTile extends WindowFunction {
        public static final FunctionProperty<Integer> N = new FunctionProperty<>("n", "N to tile from. Equivalent to NTILE function in SQL");

        @Override
        public String getColumnPrefix() {
            return "_n_tile";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters) {
            Integer nValue = checkAndGetProperty(parameters, N);
            return functions.ntile(nValue);
        }
    }

    public static class RowNumber extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_row_num";
        }

        @Override
        protected Column apply() {
            return functions.row_number();
        }
    }

    public static class Test extends WindowFunction {
        @Override
        public String getColumnPrefix() {
            return "_row_num";
        }

        @Override
        protected Column apply(Map<String, Serializable> parameters) {
            return functions.row_number();
        }
    }
}