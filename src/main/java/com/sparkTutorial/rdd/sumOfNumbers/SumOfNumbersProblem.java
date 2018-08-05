package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;

import java.util.Arrays;
import java.util.List;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("reduce").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> integers_sc = sc.textFile("in/prime_nums.text");
//        set as itterator
        JavaRDD<String> numbers = integers_sc.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
//        remove all white spaces and convert to integer
        JavaRDD<Integer> clean_numbers = numbers.filter(x -> !x.isEmpty()).map(y -> Integer.valueOf(y));
        Integer answer = clean_numbers.reduce((x,y)->x +y);
        System.out.println("The answer is: " + answer);




        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */


    }
}
