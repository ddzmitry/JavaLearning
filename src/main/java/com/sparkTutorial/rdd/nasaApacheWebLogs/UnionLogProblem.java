package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogProblem {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("UnionLogProblem").setMaster("local[4]");
//        Create spark context first
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");
//        make an aggregation of the files
        JavaRDD<String> agregaredLines = julyFirstLogs.union(augustFirstLogs);
        JavaRDD<String> cleanLines = agregaredLines.filter(line -> isNoHeader(line));
        JavaRDD<String> sample = cleanLines.sample(true,0.1);
        sample.saveAsTextFile("out/sample_nasa_logs.csv");
    }

    private static boolean isNoHeader(String line) {return !(line.startsWith("host") && line.contains("bytes"));}
}
