# Transform MessageHub events into Parquet files in Object Storage

This project allows you to pull events from MessageHub and, using Spark Streaming, save them in Object Storage for further processing.

To compile and run this code execute:
```bash
sbt assembly
```
and then run
```bash
./spark-submit.sh --vcap ./vcap.json --deploy-mode cluster --class com.ibm.cicto.streaming.MHBXSparkStreamingJob --master https://169.54.219.20 target/scala-2.10/MessageHub_Spark_Bluemix-assembly-1.0.jar
```

This uploads the jar file and tells the Spark service to start processing events from MessageHub.  
