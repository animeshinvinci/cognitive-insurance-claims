package com.ibm.bpm.cloud.ci.cto.prediction

import spark.jobserver.SparkJob
import org.apache.spark.ml.PipelineModel
import spark.jobserver.SparkJobValid
import org.apache.spark.SparkContext
import org.apache.spark.ml.tuning.CrossValidatorModel
import spark.jobserver.SparkJobInvalid
import org.apache.spark.ml.classification.RandomForestClassificationModel
import spark.jobserver.SparkJobValidation
import spark.jobserver.NamedObjectPersister
import spark.jobserver.NamedObjectSupport
import com.typesafe.config.Config

class GetLoanModel extends SparkJob with NamedObjectSupport {
  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val cvModel = this.namedObjects.get[NamedModel]("model:loanModel").get.model.asInstanceOf[CrossValidatorModel]
    val accuracy = this.namedObjects.get[NamedDouble]("accuracy:loanAccuracy").get.double
    var modelString = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString
    modelString = modelString.replaceAll("feature 0", "loanAmount")
    modelString = modelString.replaceAll("feature 1", "creditScore")
    modelString = modelString.replaceAll("feature 2", "customerType")
    modelString = modelString.replaceAll(" 0\\.0", " reject")
    modelString = modelString.replaceAll(" 1\\.0", " approve")
    modelString = modelString.replaceAll("\\n", "<br>")
    modelString = modelString.replaceAll("\\s", "&nbsp;")
    //println("Best Model is: \n" + modelString)
    //modelString.to
    var m: Map[String, Any] = Map("bestModel" -> modelString, "accuracy" -> accuracy * 100.0)
    (m)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    val obj = this.namedObjects.get("model:loanModel")
    val names = this.namedObjects.getNames()
    names.foreach(println)
    if (obj.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named object [model:loanModel]")
  }
}