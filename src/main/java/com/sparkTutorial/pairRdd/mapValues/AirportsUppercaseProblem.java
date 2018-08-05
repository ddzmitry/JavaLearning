package com.sparkTutorial.pairRdd.mapValues;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class AirportsUppercaseProblem {

    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports_data = sc.textFile("in/airports.text");
        JavaPairRDD<String,String> airport_tuple_data = airports_data.mapToPair(getLinedata());
        JavaPairRDD<String,String> airport_caps_data = airport_tuple_data.mapValues(x -> x.toUpperCase());
        airport_caps_data.collect().forEach(x -> {
            System.out.println(x);
        });

        airport_caps_data.saveAsTextFile("out/all_airport_caps.text");

        /* Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
           being the key and country name being the value. Then convert the country name to uppercase and
           output the pair RDD to out/airports_uppercase.text

           Each row of the input file contains the following columns:

           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "CANADA")
           ("Wewak Intl", "PAPUA NEW GUINEA")
           ...
         */
    }

//    annotate function
    private static PairFunction<String,String,String> getLinedata() {
//        return tuple
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1],
                line.split(Utils.COMMA_DELIMITER)[3]);
    }
}
