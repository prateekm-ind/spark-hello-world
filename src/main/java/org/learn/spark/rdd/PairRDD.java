package org.learn.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/***
 * - Create a RAW Log message into RDD's
 *
 */
public class PairRDD {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<String> inputData = new ArrayList<>();

        inputData.add("WARN: Tuesday 4th September 2022");
        inputData.add("DEBUG: Friday 4th October 2022");
        inputData.add("FATAL: Tuesday 4th November 2022");
        inputData.add("ERROR: Wednesday 29th March 2023");
        inputData.add("INFO: Friday 31th March 2023");
        inputData.add("ERROR: Saturday 1st April 2023");
        inputData.add("WARN: Tuesday 3rd April 2023");

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /**
         * Step 1: Convert the raw message into initial RDD messages
         */
        JavaRDD<String> originalLogMessage = sc.parallelize(inputData);

        /**
         * Step 2: Convert the String RDD into the desired form
         * here JavaPairRDD<String, String>
         */
        JavaPairRDD<String, String> pairRDD = originalLogMessage.mapToPair(rawavalue -> {
            String[] columns = rawavalue.split(":");
            String day = columns[0];
            String date = columns[1];

            return new Tuple2<String, String>(day, date);
        });

        pairRDD.collect().forEach(System.out::println);


        /**
         * groupByKey()
         * - pair up all the values based on the keys
         */
        JavaPairRDD<String, Iterable<String>> stringIterableJavaPairRDD = pairRDD.groupByKey();

        stringIterableJavaPairRDD.collect().forEach(System.out::println);


        /**
         * Reduce By Key
         * - PairRDD<String, Integer>
         * - all keys are gathered before reducing them
         * - have better performance over groupByKey() for count purposes
         */
        JavaPairRDD<String, Long> javaPairRDD = originalLogMessage.mapToPair( rawValue -> {
            String[] columns = rawValue.split(" : ");
            String level = columns[0];
            return new Tuple2<>(level, 1L);
        });

        javaPairRDD.collect().forEach(System.out::println);

        JavaPairRDD<String, Long> reducedPairRDD = javaPairRDD.reduceByKey((val1, val2) -> val1 + val2);

        reducedPairRDD.collect().forEach(System.out::println);

        /**
         *
         * flatMap RDD
         */

        System.out.println(" *** Filter and FlatMap Operations *** ");
        JavaRDD<String> words = originalLogMessage.flatMap(values -> Arrays.asList(values.split(" ")).iterator());

        JavaRDD<String> filteredWords = words.filter(word -> word.length()>7);

        filteredWords.collect().forEach(System.out::println);
        //words.collect().forEach(System.out::println);

        sc.close();
    }
}
