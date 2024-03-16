package org.learn.spark.rdd;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.log4j.Level.WARN;

public class RDDJoinExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(WARN);

        SparkConf sparkConf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();

        visitsRaw.add(new Tuple2<>(4,18));
        visitsRaw.add(new Tuple2<>(6,4));
        /**
         * As there is no match in the usersRaw and visitsRaw this will not be added into the resultant joinedRDD
         */
        visitsRaw.add(new Tuple2<>(10, 9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3,"Alan"));
        usersRaw.add(new Tuple2<>(4,"Doris"));
        usersRaw.add(new Tuple2<>(5,"Marybelle"));
        usersRaw.add(new Tuple2<>(6,"Raquel"));


        JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);

        /**
         * Performing innerJoin()
         *
         */
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRDD = visits.join(users);

        joinedRDD.collect().forEach(System.out::println);


        /**
         * Performing leftOuterJoin()
         */
        System.out.println("**** ---- Left Outer Join ---- ****");
        JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoinRDD = visits.leftOuterJoin(users);

        leftOuterJoinRDD.collect().forEach(System.out::println);

        /**
         * Performing rightOuterJoin()
         */
        System.out.println("**** ---- Right Outer Join ---- ****");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoinRDD = visits.rightOuterJoin(users);

        rightOuterJoinRDD.collect().forEach(System.out::println);

        /**
         * Performing fullOuterJoin
         */
        System.out.println("**** ---- Full Outer Join ---- ****");
        JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoin = visits.fullOuterJoin(users);

        fullOuterJoin.collect().forEach(System.out::println);

        sc.close();


    }
}
