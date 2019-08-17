/*---------------------------------------------------------------------------------------------*/
/*----The below code will load the data for the temperature observations from 1756 to 2017 
 into different tables for different ranges of data available{(1756-1858),(1859-1960),(1961-2012),
 (2013-2017, manual station),(2013-2017, automatic station)}----*/
/*---------------------------------------------------------------------------------------------*/

package package com.implement.spark.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.log4j.Logger

class temperatureLoadExecutor(var spark: SparkSession) {
  val logger =  Logger.getLogger(this.getClass.getName)
  
  def execute {
    
   /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 1756 TO 1858---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1756 TO 1858")

    val temperatureDataset1 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd1 = spark.sparkContext.parallelize(temperatureDataset1)

    logger.info("defining schema FOR THE RANGE 1756 TO 1858")

    val dfSchema1 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 1756 TO 1858")

    val finalTemperatureDatasetRdd1 = temperatureDatasetRdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6))) //0th column not taken because it is blank
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1756 TO 1858")

    val temperatureDatasetDf1 = spark.createDataFrame(finalTemperatureDatasetRdd1, dfSchema1)

    val finalTemperatureDatasetDf1 = temperatureDatasetDf1.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf1.createOrReplaceTempView("finalTemperatureDatasetDf1Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/
    spark.sql("""CREATE TABLE if not exists default.temp_obs_1756_1858_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double) stored as parquet location 'C:\\SampleDataWrite\\temp_obs_1756_1858_parq'""")

    spark.sql("insert overwrite table default.temp_obs_1756_1858_parq select * from finalTemperatureDatasetDf1Table")

    /*Method : 2*/

    finalTemperatureDatasetDf1.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1756_1858_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 1859 TO 1960---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1859 TO 1960")

    val temperatureDataset2 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd2 = spark.sparkContext.parallelize(temperatureDataset2)

    logger.info("defining schema FOR THE RANGE 1859 TO 1960")

    val dfSchema2 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 1859 TO 1960")

    val finalTemperatureDatasetRdd2 = temperatureDatasetRdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8)
      .map(r => r.split(",")).map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1859 TO 1960")

    val temperatureDatasetDf2 = spark.createDataFrame(finalTemperatureDatasetRdd2, dfSchema2)

    val finalTemperatureDatasetDf2 = temperatureDatasetDf2.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf2.createOrReplaceTempView("finalTemperatureDatasetDf2Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.temp_obs_1859_1960_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double, TempMinC double, TempMaxC double) stored as parquet location 'C:\\SampleDataWrite\\temp_obs_1859_1960_parq'""")

    spark.sql("insert overwrite table default.temp_obs_1859_1960_parq select * from finalTemperatureDatasetDf2Table")

    /*Method : 2*/

    finalTemperatureDatasetDf2.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1859_1960_parq_1")
       
/*---------------------------------------------------------------------------------------------------------------*/    
        
 /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 1961 TO 2012---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1961 TO 2012")

    val temperatureDataset3 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd3 = spark.sparkContext.parallelize(temperatureDataset3)

    logger.info("defining schema FOR THE RANGE 1961 TO 2012")

    val dfSchema3 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 1961 TO 2012")

    val finalTemperatureDatasetRdd3 = temperatureDatasetRdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
        .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
        .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
        .map(r => r.split(","))
        .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))


    logger.info("creating dataframe FOR THE RANGE 1961 TO 2012")

    val temperatureDatasetDf3 = spark.createDataFrame(finalTemperatureDatasetRdd3, dfSchema3)

    val finalTemperatureDatasetDf3 = temperatureDatasetDf3.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf3.createOrReplaceTempView("finalTemperatureDatasetDf3Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/
    

    spark.sql("""CREATE TABLE if not exists default.temp_obs_1961_2012_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double, TempMinC double, TempMaxC double, TempMeanC double) stored as parquet location 'C:\\SampleDataWrite\\temp_obs_1961_2012_parq'""")

    spark.sql("insert overwrite table default.temp_obs_1961_2012_parq select * from finalTemperatureDatasetDf3Table")

    /*Method : 2*/

    finalTemperatureDatasetDf3.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_1961_2012_parq_1")
       
/*---------------------------------------------------------------------------------------------------------------*/          
        
 /*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file RANGE 2013 TO 2017 FROM MANUAL STATION")

    val temperatureDataset4 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd4 = spark.sparkContext.parallelize(temperatureDataset4)

    logger.info("defining schema RANGE 2013 TO 2017 FROM MANUAL STATION")

    val dfSchema4 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion RANGE 2013 TO 2017 FROM MANUAL STATION")

    val finalTemperatureDatasetRdd4 = temperatureDatasetRdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
        .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
        .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
        .map(r => r.split(","))
        .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))




    logger.info("creating dataframe RANGE 2013 TO 2017 FROM MANUAL STATION")

    val temperatureDatasetDf4 = spark.createDataFrame(finalTemperatureDatasetRdd4, dfSchema4)

    val finalTemperatureDatasetDf4 = temperatureDatasetDf4.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf4.createOrReplaceTempView("finalTemperatureDatasetDf4Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/
    
    spark.sql("""CREATE TABLE if not exists default.temp_obs_2013_2017_manual_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double, TempMinC double, TempMaxC double, TempMeanC double) stored as parquet location 'C:\\SampleDataWrite\\temp_obs_2013_2017_manual_parq'""")

    spark.sql("insert overwrite table default.temp_obs_2013_2017_manual_parq select * from finalTemperatureDatasetDf4Table")

    /*Method : 2*/

    finalTemperatureDatasetDf4.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_2013_2017_manual_parq_1")
       
/*---------------------------------------------------------------------------------------------------------------*/        

/*---------------------TEMPERATURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val temperatureDataset5 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
      .mkString
      .split("\n")

    val temperatureDatasetRdd5 = spark.sparkContext.parallelize(temperatureDataset5)

    logger.info("defining schema FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val dfSchema5 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("TempMinC", DoubleType, true),
        StructField("TempMaxC", DoubleType, true),
        StructField("TempMeanC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val finalTemperatureDatasetRdd5 = temperatureDatasetRdd5.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
        .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
        .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
        .map(r => r.split(","))
        .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))


 logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val temperatureDatasetDf5 = spark.createDataFrame(finalTemperatureDatasetRdd5, dfSchema5)

    val finalTemperatureDatasetDf5 = temperatureDatasetDf5.coalesce(1) //This is done to reduce shuffling

    finalTemperatureDatasetDf5.createOrReplaceTempView("finalTemperatureDatasetDf5Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/
    
    spark.sql("""CREATE TABLE if not exists default.temp_obs_2013_2017_auto_parq(
     Year int, Month int, Day int, MorningTempC double, NoonTempC double, EveningTempC double, TempMinC double, TempMaxC double, TempMeanC double) stored as parquet location 'C:\\SampleDataWrite\\temp_obs_2013_2017_auto_parq'""")

    spark.sql("insert overwrite table default.temp_obs_2013_2017_auto_parq select * from finalTemperatureDatasetDf5Table")

    /*Method : 2*/

    finalTemperatureDatasetDf5.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\temp_obs_2013_2017_auto_parq_1")
       
/*---------------------------------------------------------------------------------------------------------------*/  
 
/*--------Validating data---------*/
    
spark.table("default.temp_obs_1756_1858_parq").show(10,false)
spark.table("default.temp_obs_1859_1960_parq").show(10,false)
spark.table("default.temp_obs_1961_2012_parq").show(10,false)
spark.table("default.temp_obs_2013_2017_manual_parq").show(10,false)
spark.table("default.temp_obs_2013_2017_auto_parq").show(10,false)

println(spark.table("default.temp_obs_1756_1858_parq").count)
println(spark.table("default.temp_obs_1859_1960_parq").count)
println(spark.table("default.temp_obs_1961_2012_parq").count)
println(spark.table("default.temp_obs_2013_2017_manual_parq").count)
println(spark.table("default.temp_obs_2013_2017_auto_parq").count)

/*--------------------------------*/
  }

}