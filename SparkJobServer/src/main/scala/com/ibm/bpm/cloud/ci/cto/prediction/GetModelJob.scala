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
import scala.util.Try

class GetModelJob extends SparkJob with NamedObjectSupport {
  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def stringPersister[T]: NamedObjectPersister[NamedString] = new StringPersister

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    //Get number of claims used for a given model
    val numClaims = this.namedObjects.get[NamedDouble]("model:claimsUsed").get.double

    val cvModel = Try(this.namedObjects.get[NamedModel]("model:claimModel").get.model.asInstanceOf[CrossValidatorModel])
      .getOrElse(this.namedObjects.get[NamedModel]("model:claimModel").get.model.asInstanceOf[PipelineModel])
    val modelType = this.namedObjects.get[NamedString]("model:modelType").get.string.asInstanceOf[String]
    val accuracy = this.namedObjects.get[NamedDouble]("accuracy:accuracy").get.double
    var modelString = "Model Type: " + modelType + "<br>"
    modelType match {
      case "kmeans" => modelString += cvModel.asInstanceOf[PipelineModel].stages(2).explainParams().replaceAll("\\n", "<br>")
      case "decisionForest" => {
        //TODO once https://issues.apache.org/jira/browse/SPARK-16857 is addressed or we get spark 2.0 running on spark-jobserver uncomment
        modelString += cvModel.asInstanceOf[CrossValidatorModel].bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString
        modelString = modelString.replaceAll("feature 0", "approvedAmount")
        modelString = modelString.replaceAll("feature 1", "estimate")
        modelString = modelString.replaceAll("feature 2", "creditScore")
        modelString = modelString.replaceAll(" 0\\.0", " reject")
        modelString = modelString.replaceAll(" 1\\.0", " approve")
        modelString = modelString.replaceAll("\\n", "<br>")
        modelString = modelString.replaceAll("\\s", "&nbsp;")
      }
      case _ => modelString += cvModel.asInstanceOf[CrossValidatorModel].bestModel.asInstanceOf[PipelineModel].stages(2).explainParams().replaceAll("\\n", "<br>")
    }

    //println("Best Model is: \n" + modelString)
    //modelString.to
    var m: Map[String, Any] = Map("bestModel" -> modelString, "accuracy" -> accuracy * 100.0, "numClaims" -> numClaims)
    (m)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    val obj = this.namedObjects.get("model:claimModel")
    val modelType = this.namedObjects.get("model:modelType")
    val names = this.namedObjects.getNames()
    names.foreach(println)
    if (obj.isDefined && modelType.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named object [model:claimModel]")
  }

}