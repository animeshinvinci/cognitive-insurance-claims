package com.ibm.cicto.helpers

import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted

/**
 * from https://github.com/ibm-cds-labs/spark.samples/blob/4c3e78b79191c011da726d92922d53c3b2fbf4f1/streaming-twitter/src/main/scala/com/ibm/cds/spark/samples/StreamingListener.scala
 */
class MyStreamingListener
    extends org.apache.spark.streaming.scheduler.StreamingListener {
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {
    println("Receiver Started: " + receiverStarted.receiverInfo.name)
  }

  override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    println("Receiver Error: " + receiverError.receiverInfo.lastError)
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {
    println("Receiver Stopped: " + receiverStopped.receiverInfo.name)
    println("Reason: " + receiverStopped.receiverInfo.lastError + " : " + receiverStopped.receiverInfo.lastErrorMessage)
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    println("Batch started with " + batchStarted.batchInfo.numRecords + " records")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    println("Batch completed with " + batchCompleted.batchInfo.numRecords + " records");
  }
}