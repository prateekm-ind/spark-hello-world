package org.learn.spark.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.learn.spark.Utils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Locale;

public class TextFileRDD {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRDD = sc.textFile("src/main/resources/inception.txt");

        initialRDD.flatMap(val -> Arrays.asList(val.split(" ")).iterator()).collect().forEach(System.out::println);

        //Long count = initialRDD.flatMap(val -> Arrays.asList(val.split("")).iterator()).reduce((x, y) -> x + y);

//       List<String> results = initialRDD.take(100);
//        results.forEach(System.out::println);

        /**
         * Allowed only alphabets replace rest with ""
         */
        JavaRDD<String> lettersOnlyRDD = initialRDD.map( sentence -> {
           return sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase(Locale.ROOT);
        });

        /**
         * Remove the white spaces in between lines
         */
        JavaRDD<String> sentenceOnlyRDD = lettersOnlyRDD.filter(sentence -> sentence.trim().length()>0);

        /**
         * Identify each words in the sentences and collect them in List
         */
        JavaRDD<String> wordsOnlyRDD = sentenceOnlyRDD.flatMap(sentences -> Arrays.asList(sentences.split(" ")).iterator());

        /**
         * FILTER out the boring words
         */
        JavaRDD<String> effectiveWords = wordsOnlyRDD.filter(words -> words.length()>0);
        JavaRDD<String> interestingWords = effectiveWords.filter( words -> Utils.isNotBoring(words));
        //interestingWords.collect().forEach(System.out::println);


        JavaPairRDD<String,Long> pairRDD = interestingWords.mapToPair(word -> new Tuple2<>(word, 1L));

        /**
         * Switch the rdd to get the keys sorted
         *
         */
        JavaPairRDD<Long, String> switchedRDD = pairRDD.reduceByKey((val1, val2)-> val1+ val2).mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        System.out.println("*** number of partitions *** : "+switchedRDD.getNumPartitions());

        switchedRDD.sortByKey(false).collect().forEach(System.out::println);

        sc.close();


    }
}
