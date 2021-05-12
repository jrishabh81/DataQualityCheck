package com.lwwale;


import com.lwwale.exception.ApplicationException;
import com.lwwale.exception.MandatoryPropertyMissingException;
import com.lwwale.functions.DQCheckType;
import com.lwwale.models.DqCheckModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class DataQualityCheckTest {

    private Dataset<Row> input;
    private SparkSession sparkSession;
    private DataQualityCheck dataQualityCheck;

    @BeforeEach
    public void setUp() {
        sparkSession = SparkSession.builder().master("local").appName("DataQualityCheckTest").enableHiveSupport().getOrCreate();
        input = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("./src/test/resources/input/input.csv");

        dataQualityCheck = new DataQualityCheck();
    }

    @AfterEach
    public void tearDown() {
        sparkSession.close();
    }

    @Test
    public void performDqCheck() {
        Map<String, Serializable> parameters = new HashMap<>();
        parameters.put("columnName", "rate");
        parameters.put("window_columns", "col1");
        parameters.put("n", 5);
        parameters.put("value", 5);
        parameters.put("l_value", 5);
        parameters.put("u_value", 5);
        parameters.put("offset", 5);
        parameters.put("default_value", 500);
        parameters.put("regex", ".*");
        List<DqCheckModel> dqCheckModels = new ArrayList<>();
        int i = 0;
        for (DQCheckType value : DQCheckType.values()) {
            DqCheckModel dqCheckModel = new DqCheckModel(value);
            dqCheckModel.setColumnName("rate")
                    .setParameters(Collections.unmodifiableMap(parameters))
                    .setSequence(i++);
            dqCheckModels.add(dqCheckModel);
        }
        Dataset<Row> dqCheck = dataQualityCheck.performDqCheck(input, dqCheckModels);

       /* JavaRDD<Double> df = input.select("rate").toJavaRDD().map((Function<Row, Double>) row -> row.getDouble(0),
        JavaRDD<Double> df2 = input.select("rate").toJavaRDD().map((Function<Row, Double>) row -> 0 - row.getDouble(0),
        double corr = Statistics.corr(df, df2);
        System.err.println(corr);*/
//        sqlContext.udf().register("test", new TestUdf(), DataTypes.StringType);
//
//        Dataset<Row> rowDataset = input.withColumn("test_column", functions.callUDF("test", input.col("col1")));
        StructField[] structFields = {
                StructField.apply("id", StringType, true, Metadata.empty()),
                StructField.apply("col1", StringType, true, Metadata.empty()),
                StructField.apply("rate", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_is_null", BooleanType, false, Metadata.empty()),
                StructField.apply("rate_is_not_null", BooleanType, false, Metadata.empty()),
                StructField.apply("rate_is_gt", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_is_geq", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_is_lt", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_is_leq", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_is_equal_to", BooleanType, false, Metadata.empty()),
                StructField.apply("rate_is_not_equal_to", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_is_NAN", BooleanType, false, Metadata.empty()),
                StructField.apply("rate_is_between", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_is_in", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_like", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_sql_like", BooleanType, true, Metadata.empty()),
                StructField.apply("rate_abs", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_lag", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_lead", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_mavg", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_corr", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_covar_pop", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_covar_samp", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_dense_rank", IntegerType, true, Metadata.empty()),
                StructField.apply("rate_percent_rank", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_rank", IntegerType, true, Metadata.empty()),
                StructField.apply("rate_avg", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_kurtosis", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_max", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_min", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_mean", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_skewness", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_std_dev", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_sum", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_variance", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_cume_dist", DoubleType, true, Metadata.empty()),
                StructField.apply("rate_n_tile", IntegerType, true, Metadata.empty()),
                StructField.apply("rate_row_num", IntegerType, true, Metadata.empty())
        };

        StructType structType = new StructType(structFields);
        Dataset<Row> output = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .schema(structType)
                .load("./src/test/resources/output/expectedOutput.csv");

        Dataset<Row> except = dqCheck.except(output);
        long count = except.count();
        assertEquals(count, 0l);

//        output.write().option("header","true").csv("expoutput");
//        input.write().option("header","true").csv("actoutput");
//        assertDatasetApproximateEquals(output, dqCheck, 0.05);

    }

    @Test
    public void testNullDataSet() {
        Assertions.assertThrows(ApplicationException.class, () ->
                dataQualityCheck.performDqCheck(null, Collections.singletonList(new DqCheckModel(DQCheckType.CORRELATION)))
        );
    }

    @Test
    public void testNoMandatoryProperty() {
        Assertions.assertThrows(MandatoryPropertyMissingException.class, () ->
                dataQualityCheck.performDqCheck(input, Collections.singletonList(new DqCheckModel(DQCheckType.LESS_THEN).setColumnName("rate")))
        );
    }


}