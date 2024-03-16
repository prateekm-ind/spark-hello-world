package org.learn.spark.exercise;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ViewingFigures {

    public static void main(String[] args) {

    }

    public static JavaPairRDD<Integer, Integer> setUpChapterDataRDD(JavaSparkContext sc, boolean testMode){
        //(chapterId, (courseId, courseTitle)
        List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
        rawChapterData.add(new Tuple2<>(96,3));
        rawChapterData.add(new Tuple2<>(97,2));
        rawChapterData.add(new Tuple2<>(98,1));
        rawChapterData.add(new Tuple2<>(99,2));
        rawChapterData.add(new Tuple2<>(100,3));
        rawChapterData.add(new Tuple2<>(101,2));
        rawChapterData.add(new Tuple2<>(102,3));
        rawChapterData.add(new Tuple2<>(103,4));
        rawChapterData.add(new Tuple2<>(105,2));
        rawChapterData.add(new Tuple2<>(106,2));

        return sc.parallelizePairs(rawChapterData);

    }

    public static JavaPairRDD<Integer, Integer> setUpViewDataRDD(JavaSparkContext sc, boolean testMode){
        List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
        //Chapter Views (userId,ChapterId)
        rawViewData.add(new Tuple2<>(14,96));
        rawViewData.add(new Tuple2<>(14,97));
        rawViewData.add(new Tuple2<>(13,101));
        rawViewData.add(new Tuple2<>(13,98));
        rawViewData.add(new Tuple2<>(14,105));
        rawViewData.add(new Tuple2<>(15,106));
        rawViewData.add(new Tuple2<>(13,100));
        rawViewData.add(new Tuple2<>(15,101));

        return sc.parallelizePairs(rawViewData);
    }

}
