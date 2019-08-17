/*---------------------------------------------------------------------------------------------*/
/*----The below code will load the data for the pressure observations from 1756 to 2017 
 into different tables for different ranges of data available{(1756-1858),(1859-1861),(1862-1937),
 (1938-1960),(1961-2012),(2013-2017,manual station),(2013-2017,automatic station)}----*/
/*---------------------------------------------------------------------------------------------*/

package package com.implement.spark.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType }
import org.apache.log4j.Logger

class pressureLoadExecutor(var spark: SparkSession) {
  val logger =  Logger.getLogger(this.getClass.getName)
  
  def execute{
    
      /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1756 TO 1858---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1756 TO 1858")

    val pressureDataset1 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1756_1858.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd1 = spark.sparkContext.parallelize(pressureDataset1)

    logger.info("defining schema FOR THE RANGE 1756 TO 1858")

    val dfSchema1 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureIn29_69mm", DoubleType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("NoonPressureIn29_69mm", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("EveningPressureIn29_69mm", DoubleType, true),
        StructField("EveningTempC", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 1756 TO 1858")

    val finalPressureDatasetRdd1 = pressureDatasetRdd1.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1756 TO 1858")

    val pressureDatasetDf1 = spark.createDataFrame(finalPressureDatasetRdd1, dfSchema1)

    val finalPressureDatasetDf1 = pressureDatasetDf1.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf1.createOrReplaceTempView("finalPressureDatasetDf1Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/
    spark.sql("drop table if exists default.pressure_obs_1756_1858_parq")
    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1756_1858_parq(
     Year int, Month int, Day int, MorningPressureInMm29_69 double, MorningTempC double, NoonPressureInMm29_69 double, NoonTempC double, EveningPressureInMm29_69 double, EveningTempC double) stored as parquet location 'C:\\SampleDataWrite\\pressure_obs_1756_1858_parq'""")

    spark.sql("insert overwrite table default.pressure_obs_1756_1858_parq select * from finalPressureDatasetDf1Table")

    /*Method : 2*/

    finalPressureDatasetDf1.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1756_1858_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1859 TO 1861---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1859 TO 1861")

    val pressureDataset2 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1859_1861.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd2 = spark.sparkContext.parallelize(pressureDataset2)

    logger.info("defining schema FOR THE RANGE 1859 TO 1861")

    val dfSchema2 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureIn2_969mm", DoubleType, true),
        StructField("MorningTempC", DoubleType, true),
        StructField("MorningPressureIn2_969mmAt0C", DoubleType, true),
        StructField("NoonPressureIn2_969mm", DoubleType, true),
        StructField("NoonTempC", DoubleType, true),
        StructField("NoonPressureIn2_969mmAt0C", DoubleType, true),
        StructField("EveningPressureIn2_969mm", DoubleType, true),
        StructField("EveningTempC", DoubleType, true),
        StructField("EveningPressureIn2_969mmAt0C", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 1859 TO 1861")

    val finalPressureDatasetRdd2 = pressureDatasetRdd2.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6), r.split(",")(7), r.split(",")(8), r.split(",")(9), r.split(",")(10), r.split(",")(11)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + "," + r._8 + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble, a(6).toDouble, a(7).toDouble, a(8).toDouble, a(9).toDouble, a(10).toDouble, a(11).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1859 TO 1861")

    val pressureDatasetDf2 = spark.createDataFrame(finalPressureDatasetRdd2, dfSchema2)

    val finalPressureDatasetDf2 = pressureDatasetDf2.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf2.createOrReplaceTempView("finalPressureDatasetDf2Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1859_1861_parq(
     Year int, Month int, Day int, MorningPressureIn2_969mm double, MorningTempC double, MorningPressureIn2_969mmAt0C double, NoonPressureIn2_969mm double, NoonTempC double, NoonPressureIn2_969mmAt0C double, EveningPressureIn2_969mm double, EveningTempC double, EveningPressureIn2_969mmAt0C double) stored as parquet location 'C:\\SampleDataWrite\\pressure_obs_1859_1861_parq'""")

    spark.sql("insert overwrite table default.pressure_obs_1859_1861_parq select * from finalPressureDatasetDf2Table")

    /*Method : 2*/

    finalPressureDatasetDf2.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1859_1861_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1862 TO 1937---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1862 TO 1937")

    val pressureDataset3 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1862_1937.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd3 = spark.sparkContext.parallelize(pressureDataset3)

    logger.info("defining schema FOR THE RANGE 1862 TO 1937")

    val dfSchema3 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInMmHg", DoubleType, true),
        StructField("NoonPressureInMmHg", DoubleType, true),
        StructField("EveningPressureInMmHg", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 1862 TO 1937")

    val finalPressureDatasetRdd3 = pressureDatasetRdd3.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1862 TO 1937")

    val pressureDatasetDf3 = spark.createDataFrame(finalPressureDatasetRdd3, dfSchema3)

    val finalPressureDatasetDf3 = pressureDatasetDf3.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf3.createOrReplaceTempView("finalPressureDatasetDf3Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1862_1937_parq(
     Year int, Month int, Day int, MorningPressureInMmHg double, NoonPressureInMmHg double, EveningPressureInMmHg double) stored as parquet location 'C:\\SampleDataWrite\\pressure_obs_1862_1937_parq'""")

    spark.sql("insert overwrite table default.pressure_obs_1862_1937_parq select * from finalPressureDatasetDf3Table")

    /*Method : 2*/

    finalPressureDatasetDf3.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1862_1937_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1938 TO 1960---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1938 TO 1960")

    val pressureDataset4 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1938_1960.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd4 = spark.sparkContext.parallelize(pressureDataset4)

    logger.info("defining schema FOR THE RANGE 1938 TO 1960")

    val dfSchema4 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 1938 TO 1960")

    val finalPressureDatasetRdd4 = pressureDatasetRdd4.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5), r.split(",")(6)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1938 TO 1960")

    val pressureDatasetDf4 = spark.createDataFrame(finalPressureDatasetRdd4, dfSchema4)

    val finalPressureDatasetDf4 = pressureDatasetDf4.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf4.createOrReplaceTempView("finalPressureDatasetDf4Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1938_1960_parq(
     Year int, Month int, Day int, MorningPressureInHPa double, NoonPressureInHPa double, EveningPressureInHPa double) stored as parquet location 'C:\\SampleDataWrite\\pressure_obs_1938_1960_parq'""")

    spark.sql("insert overwrite table default.pressure_obs_1938_1960_parq select * from finalPressureDatasetDf4Table")

    /*Method : 2*/

    finalPressureDatasetDf4.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1938_1960_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 1961 TO 2012---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 1961 TO 2012")

    val pressureDataset5 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1961_2012.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd5 = spark.sparkContext.parallelize(pressureDataset5)

    logger.info("defining schema FOR THE RANGE 1961 TO 2012")

    val dfSchema5 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 1961 TO 2012")

    val finalPressureDatasetRdd5 = pressureDatasetRdd5.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 1961 TO 2012")

    val pressureDatasetDf5 = spark.createDataFrame(finalPressureDatasetRdd5, dfSchema5)

    val finalPressureDatasetDf5 = pressureDatasetDf5.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf5.createOrReplaceTempView("finalPressureDatasetDf5Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_1961_2012_parq(
     Year int, Month int, Day int, MorningPressureInHPa double, NoonPressureInHPa double, EveningPressureInHPa double) stored as parquet location 'C:\\SampleDataWrite\\pressure_obs_1961_2012_parq'""")

    spark.sql("insert overwrite table default.pressure_obs_1961_2012_parq select * from finalPressureDatasetDf5Table")

    /*Method : 2*/

    finalPressureDatasetDf5.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_1961_2012_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

    val pressureDataset6 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_2013_2017.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd6 = spark.sparkContext.parallelize(pressureDataset6)

    logger.info("defining schema FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

    val dfSchema6 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

    val finalPressureDatasetRdd6 = pressureDatasetRdd6.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM MANUAL STATION")

    val pressureDatasetDf6 = spark.createDataFrame(finalPressureDatasetRdd6, dfSchema6)

    val finalPressureDatasetDf6 = pressureDatasetDf6.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf6.createOrReplaceTempView("finalPressureDatasetDf6Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_2013_2017_manual_parq(
     Year int, Month int, Day int, MorningPressureInHPa double, NoonPressureInHPa double, EveningPressureInHPa double) stored as parquet location 'C:\\SampleDataWrite\\pressure_obs_2013_2017_manual_parq'""")

    spark.sql("insert overwrite table default.pressure_obs_2013_2017_manual_parq select * from finalPressureDatasetDf6Table")

    /*Method : 2*/

    finalPressureDatasetDf6.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_2013_2017_manual_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    /*---------------------PRESSURE OBSERVATION LOADING FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION---------------------------------*/

    logger.info("downloading data from link and creating rdd from the text file FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val pressureDataset7 = scala.io.Source.fromURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholmA_barometer_2013_2017.txt")
      .mkString
      .split("\n")

    val pressureDatasetRdd7 = spark.sparkContext.parallelize(pressureDataset7)

    logger.info("defining schema FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val dfSchema7 = StructType(
      Array(
        StructField("Year", IntegerType, true),
        StructField("Month", IntegerType, true),
        StructField("Day", IntegerType, true),
        StructField("MorningPressureInHPa", DoubleType, true),
        StructField("NoonPressureInHPa", DoubleType, true),
        StructField("EveningPressureInHPa", DoubleType, true)))

    /*Removing multiple tabs,spaces and preparing the data in "," delimited format
This is done because code was failing during data type conversion*/

    logger.info("creating final data with data type conversion FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val finalPressureDatasetRdd7 = pressureDatasetRdd7.map(r => r.replaceAll("""[\t\p{Zs}]+""", ","))
      .map(r => (r.split(",")(0), r.split(",")(1), r.split(",")(2), r.split(",")(3), r.split(",")(4), r.split(",")(5)))
      .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6)
      .map(r => r.split(","))
      .map(a => Row(a(0).toInt, a(1).toInt, a(2).toInt, a(3).toDouble, a(4).toDouble, a(5).toDouble))

    logger.info("creating dataframe FOR THE RANGE 2013 TO 2017 FROM AUTOMATIC STATION")

    val pressureDatasetDf7 = spark.createDataFrame(finalPressureDatasetRdd7, dfSchema7)

    val finalPressureDatasetDf7 = pressureDatasetDf7.coalesce(1) //This is done to reduce shuffling

    finalPressureDatasetDf7.createOrReplaceTempView("finalPressureDatasetDf7Table")

    /*Below are the two method for conversion to parquet format
1. creating the table in parquet format and then inserting the data from the temporary table
2. directly write the data of the dataframe in a location
*/

    /*Method : 1*/

    spark.sql("""CREATE TABLE if not exists default.pressure_obs_2013_2017_auto_parq(
     Year int, Month int, Day int, MorningPressureInHPa double, NoonPressureInHPa double, EveningPressureInHPa double) stored as parquet location 'C:\\SampleDataWrite\\pressure_obs_2013_2017_auto_parq'""")

    spark.sql("insert overwrite table default.pressure_obs_2013_2017_auto_parq select * from finalPressureDatasetDf7Table")

    /*Method : 2*/

    finalPressureDatasetDf7.write.mode(SaveMode.Overwrite).parquet("C:\\SampleDataWrite\\pressure_obs_2013_2017_auto_parq_1")

    /*---------------------------------------------------------------------------------------------------------------*/

    /*--------Validating data---------*/

    spark.table("default.pressure_obs_1756_1858_parq").show(10, false)
    spark.table("default.pressure_obs_1859_1861_parq").show(10, false)
    spark.table("default.pressure_obs_1862_1937_parq").show(10, false)
    spark.table("default.pressure_obs_1938_1960_parq").show(10, false)
    spark.table("default.pressure_obs_1961_2012_parq").show(10, false)
    spark.table("default.pressure_obs_2013_2017_manual_parq").show(10, false)
    spark.table("default.pressure_obs_2013_2017_auto_parq").show(10, false)

    println(spark.table("default.pressure_obs_1756_1858_parq").count)
    println(spark.table("default.pressure_obs_1859_1861_parq").count)
    println(spark.table("default.pressure_obs_1862_1937_parq").count)
    println(spark.table("default.pressure_obs_1938_1960_parq").count)
    println(spark.table("default.pressure_obs_1961_2012_parq").count)
    println(spark.table("default.pressure_obs_2013_2017_manual_parq").count)
    println(spark.table("default.pressure_obs_2013_2017_auto_parq").count)

    /*--------------------------------*/
  }

}