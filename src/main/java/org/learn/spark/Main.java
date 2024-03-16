package org.learn.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class Main {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger logger = Logger.getLogger("org.apache");

        List<Double> doubles = new ArrayList<>();

        doubles.add(32.12);
        doubles.add(23.13);
        doubles.add(923.23);
        doubles.add(423.55);

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<Double> javaRDD = sc.parallelize(doubles);

        /**
         * Reduce returns the same type it consumes
         * - T reduce(Function<T, T, T>);
         */
        Double result = javaRDD.reduce((x,y)-> x+y);

        /**
         * Math.sqrt(x)
         * this applied on every individual element of the List<Double>
         */
        JavaRDD<Double> sqrtRdd = javaRDD.map( x-> Math.sqrt(x));

        sqrtRdd.foreach( x -> System.out.println(x));

        System.out.println("** === **");

        System.out.println("Result : "+ result);

        /**
         * How many elemets in sqrtRdd
         * using just map and reduce
         */

        System.out.println("* * * Counting number of elements in a RDD * * *");
        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        Long count = singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        System.out.println(count);

        sc.close();


    }
}
