package com.sparkTutorial.pairRdd.sort;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.lang.reflect.Array;
import java.util.Arrays;

public class SortedWordCountProblem {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("SortedWordCountSolution").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> pairs = words.mapToPair(line -> new Tuple2<>(line,1));

        JavaPairRDD<String,Integer> byKey_word = pairs.reduceByKey((x,y)-> x  + y);
//        byKey_word.collect().forEach(x -> System.out.println(x));
        JavaPairRDD<Integer,String> count_byKey_word = byKey_word.mapToPair(x -> new Tuple2<>(x._2,x._1));

        JavaPairRDD<String,Integer> total_answer = count_byKey_word.sortByKey(false).mapToPair( x -> new Tuple2<>(x._2,x._1));

        for (Tuple2<String, Integer> wordToCount : total_answer.collect()) {
            System.out.println(wordToCount._1() + " : " + wordToCount._2());
        }

    }
    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
}

