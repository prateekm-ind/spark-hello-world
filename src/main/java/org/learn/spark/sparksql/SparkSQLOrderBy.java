package org.learn.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLOrderBy {

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
         * cast() function is generally used for casting a format into another format of data
         * used internally by the date_format to cast string data from date-and-time format or timestamp
         *
         */

        Dataset<Row> dateSet = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, " +
                " cast(first(date_format(datetime,'M')) as int) as monthnum, " +
                " count(1) as total from logging_table " +
                " group by level, month " +
                " order by monthnum ");

        dateSet.show();

        //optimized version

        /**
         * we can directly use sny function in the order by clause
         * hence we can skip the printing of monthnum column just for using it as a reference for the sorting
         *
         * here level is the secondary sorting performed on the dataset primary being the month-number
         * defined inside the cast()
         */
        Dataset<Row> dateSet02 = sparkSession.sql("select level, date_format(datetime,'MMMM') as month, " +
                " count(1) as total from logging_table " +
                " group by level, month " +
                " order by  cast(first(date_format(datetime,'M')) as int), level ");

        dateSet02.show();
    }
}
