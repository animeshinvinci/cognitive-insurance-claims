# Query Tracked Fields in Spark
This is a template of how you could get at tracked fields via Spark.  This project includes sample [case](src/main/scala/LoanAppCompletion.scala) and [helper](src/main/scala/LoanAppCompletionHelper.scala) classes for the LoanAppCompletion tracking group from the PerBraMex demo.

It also includes:
1. a [Spark Streaming job](src/main/scala/StreamingScalaJob.scala) which pulls DEF events from a Kafka topic, selects only the tracking group events for LoanAppCompletion, translates the events into case classes, and stores the events in HDFS as a Parquet file.
2. a [Spark job](src/main/scala/QuerySparkDataInHDFS.scala) which queries the Parquet file for KPI generation 
3. a [Spark Streaming job](src/main/scala/StreamDashboardInHDFS.scala) which emulates a dashboard by pulling the Parquet file data and joining it with the Kafka events.