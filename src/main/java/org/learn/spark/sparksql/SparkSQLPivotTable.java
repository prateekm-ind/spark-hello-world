package org.learn.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;

public class SparkSQLPivotTable {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test-spark-sql-app")
                .master("local[*]")
                .getOrCreate();


        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        Dataset<Row> dataset = sparkSession.read()
                .option("header", true)
                .csv("src/main/resources/biglog.csv");


        Dataset<Row> exprs = dataset.selectExpr("level", "date_format(datetime, 'MMMM') as month");

        exprs.show();

        /**
         * Recreating the same dataset using the java functions as we have done using the SparkSQL syntax
         *
         */
        Dataset<Row> levelAndDate = dataset.select(functions.col("level"),
                functions.date_format(functions.col("datetime"), "MMMM").alias("month"),
                functions.date_format(functions.col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType));

        levelAndDate = levelAndDate.groupBy(functions.col("level"), functions.col("month"), functions.col("monthnum"))
                .count()
                .orderBy(functions.col("monthnum"), functions.col("level"))
                .drop(functions.col("monthnum"));


        levelAndDate.show();
    }
}
