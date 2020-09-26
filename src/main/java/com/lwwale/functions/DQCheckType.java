package com.lwwale.functions;

import com.lwwale.functions.impl.ComparisionFunctions;
import com.lwwale.functions.impl.WindowFunctions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Map;

public enum DQCheckType implements Serializable {
    NULL_CHECK(new ComparisionFunctions.NullCheckFunction()),
    NOT_NULL_CHECK(new ComparisionFunctions.NotNullCheckFunction()),
    GREATER_THEN(new ComparisionFunctions.GreaterThanFunction()),
    GREATER_THEN_EQUAL_TO(new ComparisionFunctions.GreaterThanEqualToFunction()),
    LESS_THEN(new ComparisionFunctions.LessThanFunction()),
    LESS_THEN_EQUAL_TO(new ComparisionFunctions.LessThanEqualToFunction()),
    EQUAL_TO(new ComparisionFunctions.IsEqualToFunction()),
    NOT_EQUAL_TO(new ComparisionFunctions.IsNotEqualToFunction()),
    IS_NOT_A_NUMBER(new ComparisionFunctions.IsNANFunction()),
    IS_BETWEEN(new ComparisionFunctions.BetweenFunction()),
    IN_BETWEEN(new ComparisionFunctions.InFunction()),
    LIKE(new ComparisionFunctions.LikeFunction()),
    SQL_LIKE(new ComparisionFunctions.SQLLikeFunction()),
    ABS(new ComparisionFunctions.Abs()),
    //window functions
    LAG(new WindowFunctions.LagFunction()),
    LEAD(new WindowFunctions.LeadFunction()),
    MOVING_AVG(new WindowFunctions.MovingAverageFunction()),
    CORRELATION(new WindowFunctions.CorrelationFunction()),
    POPULATION_COVARIANCE(new WindowFunctions.PopulationCovariance()),
    SAMPLE_COVARIANCE(new WindowFunctions.SampleCovariance()),
    DENSE_RANK(new WindowFunctions.DenseRank()),
    PERCENT_RANK(new WindowFunctions.PercentRank()),
    RANK(new WindowFunctions.Rank()),
    AVERAGE(new WindowFunctions.Average()),
    KURTOSIS(new WindowFunctions.Kurtosis()),
    MAX(new WindowFunctions.Max()),
    MIN(new WindowFunctions.Min()),
    MEAN(new WindowFunctions.Mean()),
    SKEWNESS(new WindowFunctions.Skewness()),
    STD_DEV(new WindowFunctions.StdDev()),
    SUM(new WindowFunctions.Sum()),
    VARIANCE(new WindowFunctions.Variance()),
    CUMULATIVE_DISTRIBUTION(new WindowFunctions.CumulativeDistribution()),
    N_TILE(new WindowFunctions.NTile()),
    ROW_NUMBER(new WindowFunctions.RowNumber()),

    TEST(new WindowFunctions.Test());

    /*-----------------------------------------------------------------------------------------------------------*/
    Function function;

    DQCheckType(Function function) {
        this.function = function;
    }

    public Dataset<Row> evaluate(Dataset<Row> dataset, String columnName, String outputColumnName, Map<String, Serializable> parameters) {
        return function.evaluate(dataset, columnName, outputColumnName, parameters);
    }

    public Dataset<Row> evaluate(Dataset<Row> dataset, String columnName, Map<String, Serializable> parameters) {
        return function.evaluate(dataset, columnName, columnName.concat(function.getColumnPrefix()), parameters);
    }


    public Dataset<Row> evaluate(Dataset<Row> dataset, Map<String, Serializable> parameters, String outputColumnName) {
        return function.evaluate(dataset, outputColumnName, parameters);
    }


    public Dataset<Row> evaluate(Dataset<Row> dataset, String outputColumnName) {
        return function.evaluate(dataset, outputColumnName);
    }
}
