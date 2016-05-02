package com.ibm.bpm.cloud.ci.cto.prediction

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.tuning.CrossValidatorModel

import com.typesafe.config.Config

import spark.jobserver.NamedObjectPersister
import spark.jobserver.NamedObjectSupport
import spark.jobserver.SparkJob
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import org.apache.spark.ml.PipelineModel

class GetModelJob extends SparkJob with NamedObjectSupport {
  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val cvModel = this.namedObjects.get[NamedModel]("model:claimModel").get.model.asInstanceOf[CrossValidatorModel]
    val accuracy = this.namedObjects.get[NamedDouble]("accuracy:accuracy").get.double
    var modelString = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString
    modelString = modelString.replaceAll("feature 0", "approvedAmount")
    modelString = modelString.replaceAll("feature 1", "estimate")
    modelString = modelString.replaceAll("feature 2", "creditScore")
    modelString = modelString.replaceAll(" 0\\.0", " reject")
    modelString = modelString.replaceAll(" 1\\.0", " approve")
    modelString = modelString.replaceAll("\\n", "<br>")
    modelString = modelString.replaceAll("\\s", "&nbsp;")
    //println("Best Model is: \n" + modelString)
    //modelString.to
    var m: Map[String, Any] = Map("bestModel" -> modelString, "accuracy" -> accuracy*100.0)
    (m)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    val obj = this.namedObjects.get("model:claimModel")
    val names = this.namedObjects.getNames()
    names.foreach(println)
    if (obj.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named object [model:claimModel]")
  }

}