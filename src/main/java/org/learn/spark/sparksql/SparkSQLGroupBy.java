package org.learn.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLGroupBy {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("test-spark-sql-app")
                .master("local[*]")
                .getOrCreate();


        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        Dataset<Row> dataset = sparkSession.read()
                .option("header", true)
                .csv("src/main/resources/biglog.csv");

        dataset.createOrReplaceTempView("logging_table");

        /**
         * whenever we do a grouping we have to use an aggregate function with that
         * if we don't use aggregate function then we end up with a big dataset object containing all the data of grouped rows
         * this means it will be impossible to then load the entire object in-memory
         */
        Dataset<Row> groupByLogging = sparkSession.sql("select level, count(datetime) from logging_table group by level");

        groupByLogging.show(100);


        /**
         * date_format(timestamp, format)
         * Converts timestamp to a value of string in the format specified by the date format
         */
        Dataset<Row> dateFormat = sparkSession.sql("select level, date_format(datetime,'yyyy') from logging_table");

        dateFormat.show();

        Dataset<Row> dateFormat02 = sparkSession.sql("select level, date_format(datetime,'yyyy-MM') from logging_table");

        dateFormat02.show();

        Dataset<Row> dateFormat03 = sparkSession.sql("select level, date_format(datetime,'y-MMMM') as month from logging_table");

        dateFormat03.show();

        System.out.println(" **** ===== ****");

        Dataset<Row> dateFormat05 = sparkSession.sql("select level, date_format(datetime, 'MMMM') as month, count(1) as total from logging_table group by level, month");

        dateFormat05.show();

        dateFormat03.createOrReplaceTempView("logging_table");

        Dataset<Row> dateFormat04 = sparkSession.sql("select level, month, count(1) as total from logging_table group by level, month");

        dateFormat04.show(100);
    }
}
