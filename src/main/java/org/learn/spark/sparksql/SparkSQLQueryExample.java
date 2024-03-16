package org.learn.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLQueryExample {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test-spark-sql-app")
                .master("local[*]")
                .getOrCreate();


        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        /**
         * Under the hood there is an RDD and operation happening on that RDD
         */
        Dataset<Row> dataset = sparkSession.read()
                .option("header", true)
                .csv("src/main/resources/annual-enterprise-survey-2021-financial-year-provisional-csv.csv");

        dataset.createOrReplaceTempView("annual_enterprise_survey");

        Dataset<Row> dataset1 = sparkSession.sql("select Year, Industry_aggregation_NZSIOC from annual_enterprise_survey where Units = 'Dollars'");

        dataset1.show();
    }
}
