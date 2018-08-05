package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
public class SameHostsProblem {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airportsLatitude").setMaster("local[4]");
//        Create spark context first
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");
        JavaRDD<String> julyFirstHosts = julyFirstLogs.map(line -> line.split("\t")[0]);
        JavaRDD<String> augustFirstHosts = augustFirstLogs.map(line -> line.split("\t")[0]);
//        now we can combine all of them in  intersrction
        JavaRDD<String> intersection = julyFirstHosts.intersection(augustFirstHosts);
        JavaRDD<String> cleanedHostIntersection = intersection.filter(host -> !host.equals("host"));

        //        cleanedHostIntersection.collect().forEach(x -> {
//            System.out.println(x);
//        });
        cleanedHostIntersection.saveAsTextFile("out/sample_intersection_nasa_logs.csv");



        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */


    }

    private static boolean isNoHeader(String line) {return !(line.startsWith("host") && line.contains("bytes"));}
}
