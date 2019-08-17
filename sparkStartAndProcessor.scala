/*---------------------------------------------------------------------------------------------*/
/*----The below code will create the spark session and invoke the functions for loading 
 the temperature and pressure observation data into different tables----*/
/*---------------------------------------------------------------------------------------------*/

package com.implement.spark.sparkdemo

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object sparkStartAndProcessor {
  val logger =  Logger.getLogger(this.getClass.getName)
  def main(args: Array[String]) {
  
    logger.info("creating spark session")
    
   val spark = SparkSession.builder()
      .master("local")                   /* before running in cluster remove this and compile */
      .appName("Atmosphere Temperature Findings")
      .config("spark.sql.catalogImplementation", "hive") /*before running in cluster replace the line with .enableHiveSupport() */
      .getOrCreate()
      
   val startTime = System.currentTimeMillis()   
    logger.info("temperature observation load start")
    
    val temperatureLoad = new temperatureLoadExecutor(spark)
      temperatureLoad.execute
    
    logger.info("temperature observation load end")
    
    logger.info("pressure observation load start")
    
   val pressureLoad = new pressureLoadExecutor(spark)
      pressureLoad.execute
    
    logger.info("pressure observation load end")
    
    val endTime = System.currentTimeMillis()
 
    println("Total Time taken for the job: " + (endTime - startTime ) / (1000) + " seconds")
    logger.info("Total Time taken for the job: " + (endTime - startTime ) / (1000) + " seconds")
       
      spark.stop()
}

}