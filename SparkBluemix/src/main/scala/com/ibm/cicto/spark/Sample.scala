package com.ibm.cicto.spark

import org.apache.spark.streaming.Seconds
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

object Sample {
  lazy val kafkaProps = new MessageHubConfig
  //var conf = new SparkConf

  def main(args: Array[String]) {
    println("setting conf")
    val conf = new SparkConf().setAppName("Cognitive Claims Streaming Job")
    println("creating spark context")
    val sc = new SparkContext(conf)
    println("setting hadoop config for Swift")
    kafkaProps.set_hadoop_config(sc)
    //Create streaming context using a checkpoint directory so we can restart at the correct spot
    //in case of failure.  This also sets up the batch size.  In this case we are doing batches every
    //5 seconds.
    kafkaProps.kafkaOptionKeys.foreach { x =>
      print("key= " + x)
      println(" value= " + kafkaProps.getConfig(x))
    }

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._
    println("Bu: once again here is the config " + sc.hadoopConfiguration.get("fs.swift.service.spark.auth.url"))
    println("getting claims")
    val events = sqlContext.read.parquet("swift://CogClaim.spark/claims.parquet").cache()
    println(events.count())
  }
}