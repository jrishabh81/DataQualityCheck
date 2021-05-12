package com.lwwale;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

//TODO
public class UdfDQTest {
    SparkSession sparkSession;

    @BeforeEach
    public void setUp() {
        sparkSession = SparkSession.builder().master("local[1]").appName(getClass().getName()).enableHiveSupport().getOrCreate();
        Dataset<Row> load = sparkSession.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("./src/test/resources/input/input.csv");
        System.out.println(load.count());

    }

    @AfterEach
    public void tearDown() {
        sparkSession.close();
    }
}
