package com.sparkTutorial.pairRdd.filter;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class AirportsNotInUsaProblem {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("create").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airports_data = sc.textFile("in/airports.text");
        JavaPairRDD<String,String> pairs_airport_country = airports_data.mapToPair(getAirportNameAndCountryNamePair());

//        Test to see the data
//        pairs_airport_country.collectAsMap().forEach((x,y)->{
//            System.out.println(x + y);
//        });

        JavaPairRDD<String,String> not_us_airports = pairs_airport_country.filter(x -> !x._2.contains("\"United States\""));
//        not_us_airports.collect().forEach(x -> {
//            System.out.println(x);
//        });
        not_us_airports.saveAsTextFile("out/not_us_airports.text");

    }
    private static PairFunction<String, String, String> getAirportNameAndCountryNamePair() {
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(Utils.COMMA_DELIMITER)[1],
                line.split(Utils.COMMA_DELIMITER)[3]);
    }
}

        /* Create a Spark program to read the airport data from in/airports.text;
           generate a pair RDD with airport name being the key and country name being the value.
           Then remove all the airports which are located in United States and output the pair RDD to out/airports_not_in_usa_pair_rdd.text

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located,
           IATA/FAA code, ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:

           ("Kamloops", "Canada")
           ("Wewak Intl", "Papua New Guinea")
           ...
         */
