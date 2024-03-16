package org.learn.spark.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class SparkSQLMain {
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

        /**
         * Prints the dataset
         */
        dataset.show();

        /**
         * Count the total number of data
         */
        long count = dataset.count();

        System.out.println("Total count of records : "+ count);

        /**
         * firstRow()
         */
        Row first = dataset.first();

        //here the get() returns Object instead of specific type class
        Object object0 = first.get(0);
        Object object1 = first.get(1);
        Object object2 = first.get(2);
        Object object3 = first.get(3);
        Object object4 = first.get(4);
        Object object5 = first.get(5);

        System.out.println(object0.toString());
        System.out.println(object1.toString());
        System.out.println(object2.toString());
        System.out.println(object3.toString());
        System.out.println(object4.toString());
        System.out.println(object5.toString());

        String industry_code_anzsic06 = first.getAs("Industry_code_ANZSIC06");

        System.out.println("Industry_code_ANZSIC06 : " + industry_code_anzsic06);

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

        result.show();

        sparkSession.close();

    }

}
