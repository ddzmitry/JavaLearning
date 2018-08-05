package com.sparkTutorial.sparkSql;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class HousePriceProblem {

    private static final String PRICE = "Price";
    private static final String PRICE_SQ_FT = "Price SQ Ft";

    public static void main(String[] args) throws Exception{


        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = SparkSession.builder().appName("HousePriceAggregation").master("local[1]").getOrCreate();
        DataFrameReader dataFrameReader = session.read();
        Dataset<Row> responses = dataFrameReader.option("header","true").csv("in/RealEstate.csv");
        System.out.println("============Print Schema============");
        responses.printSchema();
        System.out.println("============Group By Location ==============");
        RelationalGroupedDataset by_location_group = responses.groupBy("Location");
        by_location_group.count().show();

        Dataset<Row> castedResponse = responses.withColumn(PRICE, col(PRICE).cast("long"))
                                                .withColumn(PRICE_SQ_FT,col(PRICE_SQ_FT).cast("long"));

        System.out.println("=============Casted Report============");
        castedResponse.show();
        System.out.println("=============Final  Report============");
        castedResponse.groupBy("Location").agg(avg(PRICE_SQ_FT), max(PRICE))
                .orderBy(col("avg(" + PRICE_SQ_FT + ")").desc())
                .show();





    }
//        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");
        /* Create a Spark program to read the house data from in/RealEstate.csv,
           group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

        +----------------+-----------------+----------+
        |        Location| avg(Price SQ Ft)|max(Price)|
        +----------------+-----------------+----------+
        |          Oceano|           1145.0|   1195000|
        |         Bradley|            606.0|   1600000|
        | San Luis Obispo|            459.0|   2369000|
        |      Santa Ynez|            391.4|   1395000|
        |         Cayucos|            387.0|   1500000|
        |.............................................|
        |.............................................|
        |.............................................|

         */
}
