package com.ibm.cicto.streaming

import org.apache.spark.streaming.Seconds
import scala.collection.JavaConversions.asJavaIterator
import scala.collection.JavaConversions.asScalaIterator
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.Map
import scala.reflect.ClassTag
import scala.reflect.classTag
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import com.ibm.cicto.helpers.MessageHubConfig
import com.ibm.cicto.helpers.KafkaStreaming.KafkaStreamingContextAdapter
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import com.ibm.cicto.helpers.ClaimComplete
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkContext
import com.ibm.cicto.helpers.KafkaReceiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.scheduler.StreamingListener
import com.ibm.cicto.helpers.MyStreamingListener

object MHBXSparkStreamingJob {
  //Need a checkpoint directory for failover.  This is in HDFS
  //val CHECKPOINT_DIR = "/data/chkpt"

  //This is an HDFS file location
  val PARQUET_FILE_LOANAPPS = "/data/loanapp.parquet"

  lazy val kafkaProps = new MessageHubConfig
  val conf = new SparkConf().setAppName("Cognitive Claims Streaming Job")
  val sc = SparkContext.getOrCreate(conf)

  def main(args: Array[String]) {
    println("Setting Hadoop/Swift config")
    kafkaProps.set_hadoop_config(sc)
    println("Hadoop/Swift config set")

    //TODO This is a hack workaround...it looks like we need to try reading/writing from sc.textFile before doing any sqlContext work
    //https://hub.jazz.net/ccm07/resource/itemName/com.ibm.team.workitem.WorkItem/156871
    println(sc.textFile("swift2d://CogClaim.keystone/nshuklatest.txt").count())


    kafkaProps.kafkaOptionKeys.foreach { x =>
      print("key= " + x)
      println(" value= " + kafkaProps.getConfig(x))
    }
    //TODO Leave out checkpointing for now, causing NPEs
    
    //println("BU " + "tenant is: " + sc.hadoopConfiguration.get("fs.swift.service.spark" + ".tenant"))
    //println("BU here is the checkpoint dir " + kafkaProps.getConfig(MessageHubConfig.CHECKPOINT_DIR_KEY))
    //val ssc = StreamingContext.getOrCreate((kafkaProps.getConfig(MessageHubConfig.CHECKPOINT_DIR_KEY)), createStreamingContext _)

    val ssc = createStreamingContext()
    
    //Set up connection to the Kafka topic.  In this case we're going to replay all events from Kafka
    //if we restart (auto.offset.reset-> smallest)
    val kafkaTopics = Set("bpmNextMMTopic")

    val streamOfEvents = ssc.createKafkaStream[String, String, StringDeserializer, StringDeserializer](kafkaProps, List("bpmNextMMTopic"))
    ssc.addStreamingListener(new MyStreamingListener())
    //val streamOfEvents = ssc.receiverStream[String, String, StringDeserializer, StringDeserializer](new KafkaReceiver(kafkaProps.toImmutableMap(), List("bpmNextMMTopic"), StorageLevel.MEMORY_AND_DISK))
    //Now process the events.
    streamOfEvents.foreachRDD(rdd => {
      if (rdd.count == 0) {
        //No events sent to kafka in this interval
        println("No events received")
      } else {
        println("Events received")
        rdd.foreach(println)
        val msg = rdd.values
        msg.foreach(println)
        //Prepare to query Kafka events to filter out the tracking group 
        //information from process navigation events
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._

        //Generate the table from the JSON.  This generates a schema we can then use later on.
        val eventTable = sqlContext.read.json(msg)
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
          //claimDF.save(PARQUET_FILE_CLAIMS, SaveMode.Append)

          //Save to ObjectStorage

          claimDF.write.mode(SaveMode.Append).save("swift2d://CogClaim.keystone/claims.parquet")
          println("Events saved in ObjectStorage")
        } catch { case e: org.apache.spark.sql.AnalysisException => /* column does not exist in a given event. noop */ }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  // Function to create and setup a new StreamingContext
  def createStreamingContext(): StreamingContext = {
    println("creating new streamingcontext")
    val ssc = new StreamingContext(sc, Seconds(10))
    println("setting checkpoint " + kafkaProps.getConfig(MessageHubConfig.CHECKPOINT_DIR_KEY))
    //TODO uncomment when checkpointing works again.
    //ssc.checkpoint(kafkaProps.getConfig(MessageHubConfig.CHECKPOINT_DIR_KEY)) // set checkpoint directory for recovery
    ssc
  }
}