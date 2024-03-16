package org.learn.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class SparkSQLFilterExample {

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


        Dataset<Row> dataset1 = sparkSession.sql("select * from dataset");

        dataset1.show();

        System.out.println("+++++++" );

        /**
         * filter method
         *
         * Dataset : Collection of rows
         * Dataset object has filter method available just as RDD
         * filter(Column condition) ~ filter(FilterFunction<Row> func) both are same
         *
         * Functionally all the filter methods works similarly
         *
         * Dataset just like RDD is immutable
         * No methods on the Dataset is going to modify the Dataset in-memory
         *
         * under the hood an RDD is being built up, basically an execution plan
         * Will spark needs to execute the execution plan data will be read in
         * And in real cluster worker nodes are going to start doing their thing
         *
         * We are not reading the data in-memory just setting up execution plan
         *
         *
         */
        Dataset<Row> rowDataset = dataset.filter("Industry_name_NZSIOC = 'Agriculture, Forestry and Fishing' AND Units='H06'");

        rowDataset.show(10);

        /**
         * Using lambda expression inside filter()
         */
        Dataset<Row> industry_code_anzsic061 = dataset.filter(row -> row.getAs("Industry_code_ANZSIC06").equals("ANZSIC06 divisions A-S (excluding classes K6330, L6711, O7552, O760, O771, O772, S9540, S9601, S9602, and S9603)"));

        industry_code_anzsic061.show(10);

        Dataset<Row>  industry_code_anzsic061_02 = dataset.filter(row -> row.getAs("Industry_code_ANZSIC06").equals("ANZSIC06 division A"));

        industry_code_anzsic061_02.show(20);

        /**
         * Using Column class
         */
        Column unitsColumn = dataset.col("Units");
        Column industry_code_anzsic061_col = dataset.col("Industry_code_ANZSIC06");

        Dataset<Row> result = dataset.filter(unitsColumn.equalTo("Dollars (millions)")
                .and(industry_code_anzsic061_col.equalTo("ANZSIC06 division A")));


        /**
         * Using static col() methods
         */
        Dataset<Row> result02 = dataset.filter(functions.col("Units").equalTo("Dollars").and(functions.col("Year").geq("2021")));

        result.show();

        System.out.println(" ******** ============= ********");

        result02.show();

        sparkSession.close();
    }
}
