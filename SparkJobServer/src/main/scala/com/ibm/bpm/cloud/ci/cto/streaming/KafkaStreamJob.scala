package com.ibm.bpm.cloud.ci.cto.streaming

import java.nio.file.Files
import java.nio.file.Paths

import scala.reflect.runtime.universe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import com.ibm.bpm.helpers.ClaimComplete
import com.typesafe.config.Config

import kafka.serializer.StringDecoder
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import spark.jobserver.SparkStreamingJob

class KafkaStreamJob extends SparkStreamingJob {

  //This is an HDFS file location
  val PARQUET_FILE_CLAIMS = "/data/claims.parquet"

  override def runJob(ssc: StreamingContext, jobConfig: Config): Any = {
    //Set up connection to the Kafka topic.  In this case we're going to replay all events from Kafka
    //if we restart (auto.offset.reset-> smallest)
    val kakfkaParams = Map[String, String](
      "bootstrap.servers" ->
        "kafka01-prod01.messagehub.services.us-south.bluemix.net:9093,kafka02-prod01.messagehub.services.us-south.bluemix.net:9093,kafka03-prod01.messagehub.services.us-south.bluemix.net:9093,kafka04-prod01.messagehub.services.us-south.bluemix.net:9093,kafka05-prod01.messagehub.services.us-south.bluemix.net:9093",
      "auto.offset.reset" -> "smallest")
    val kafkaTopics = Set("bpmNextMMTopic")

    //Get the stream of events from the Kafka topic
    val streamOfEvents = KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kakfkaParams, kafkaTopics)
      .map(_._2)
    //Now process the events.
    streamOfEvents.foreachRDD((rdd: RDD[String]) => {
      if (rdd.count == 0) {
        //No events sent to kafka in this interval
        println("No events received")
      } else {
        //Prepare to query Kafka events to filter out the tracking group 
        //information from process navigation events
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._

        //Generate the table from the JSON.  This generates a schema we can then use later on.
        val eventTable = sqlContext.read.json(rdd)
        try {
          //Here we filter out only those events related to the tracking event LoanAppCompletion.
          val trackedFields = eventTable
            .filter(eventTable("monitorEvent.applicationData.tracking-point.groupName") === "ClaimComplete")
            .select(eventTable("monitorEvent.applicationData.tracking-point.tracked-field"))
          //Get the tracked field content
          val fields = trackedFields.select(trackedFields("tracked-field.content"))
          val fieldArray = fields.map(r => r.getAs[Seq[String]]("content"))
          //Now map the tracked field into a Scala case class called LoanAppCompletion.  
          //Mapping data into case classes makes it easier to query data and gives you some
          //helpers when manipulating the data.  I generated this case class by modifying the
          //BPM Analyzer.  This creates an RDD[LoanAppCompletion]
          val claims = fieldArray.map(r => {
            println(r)
            ClaimComplete(r(0).toLong, r(1), r(2), r(3).toLong, r(4), r(5).toLong, r(6).toLong, r(7), r(8), r(9), r(10))
          })
          //Convert the RDD into a DataFrame
          val claimDF = claims.toDF()
          //Register the DataFrame as a temporary table.  This lets us run SQL against the data
          //          claimDF.registerTempTable("ClaimCompletes")
          //          claimDF.show

          println("Executing save")
          //Save the data to HDFS, appending this stream's information.  Similar to "Insert into"
          claimDF.save(PARQUET_FILE_CLAIMS, SaveMode.Append)
          println("Events saved in HDFS")

        } catch { case e: org.apache.spark.sql.AnalysisException => /* column does not exist in a given event. noop */ }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def validate(sc: StreamingContext, config: Config): SparkJobValidation = {
    if (Files.exists(Paths.get(PARQUET_FILE_CLAIMS))) SparkJobValid else SparkJobInvalid(s"Missing claim parquet file")
    SparkJobValid
  }
}