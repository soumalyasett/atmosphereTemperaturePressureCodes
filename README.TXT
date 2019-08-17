This file describes the process to be followed for loading the temperature and pressure observations into particular tables or locations in hdfs. The location has to be editted for running the same code in the cluster.

1. Build the jar and copy it to any path of the edge node of the cluster
2. Also copy the hive-site.xml in the same path, this will be needed for accessing the hive database
3. create the log4j.properties and loadTrigger.sh file in the same path
4. Then run the shell script(sh loadTrigger.sh). This will do the spark submit in spark version 2 and invoke the scala object sparkStartAndProcessor.This will again invoke the temperature load and pressure load functions(class : temperatureLoadExecutor,pressureLoadExecutor ).Default memory configuration will be picked up.
5. After the completion of the job, a mail will be triggered based upon success or failure .(edit the mail id accordingly)
