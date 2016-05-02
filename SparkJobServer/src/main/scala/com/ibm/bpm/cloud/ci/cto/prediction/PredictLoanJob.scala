package com.ibm.bpm.cloud.ci.cto.prediction

import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.feature.StringIndexerModel
import spark.jobserver.SparkJobValid
import org.apache.spark.SparkContext
import org.apache.spark.ml.tuning.CrossValidatorModel
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValidation
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import spark.jobserver.NamedObjectPersister
import com.typesafe.config.Config
import com.ibm.bpm.helpers.ClaimComplete
import spark.jobserver.SparkJob
import spark.jobserver.NamedObjectSupport
import org.apache.spark.mllib.linalg.DenseVector

class PredictLoanJob extends SparkJob with NamedObjectSupport {
  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def assemblerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val input = jobConfig.getObject("input")
    val approvedAmount = jobConfig.getLong("input.approvedAmount")
    //println(approvedAmount)
    val estimate = jobConfig.getLong("input.estimate")
    //println(estimate)
    val creditScore = jobConfig.getLong("input.creditScore")
    //println(creditScore)
    val request = Seq(approvedAmount, estimate, creditScore)
    val claim = sc.parallelize(List(request))
    //println(claim)
    val claimDF = claim.map(c => ClaimComplete(c(0), "", "", 0, "", c(2), c(1), "", "", "", "false")).toDF
    //claimDF.printSchema()
    //claimDF.show
    //sqlContext

    val cvModel = this.namedObjects.get[NamedModel]("model:claimModel").get.model.asInstanceOf[CrossValidatorModel]

    val assembler = this.namedObjects.get[NamedTransformer]("assembler:assembler").get.transformer.asInstanceOf[VectorAssembler]
    val labelIndexerModel = this.namedObjects.get[NamedModel]("indexer:labelIndexer").get.model.asInstanceOf[StringIndexerModel]
    val featureIndexerModel = this.namedObjects.get[NamedModel]("indexer:featureIndexer").get.model.asInstanceOf[VectorIndexerModel]

    val vectoredTable = assembler.transform(claimDF)
    val prediction = cvModel.transform(vectoredTable)
    prediction.show()

    //val vectoredTable = assembler.transform(claimData)

    //val creditIndexer = this.namedObjects.get[NamedModel]("model:vehicleTypeIndexer").get.model.asInstanceOf[StringIndexerModel]

    //cvModel.
    val result = prediction.select("probability", "predictedLabel").collect
    var m: Map[String, Any] = Map()
    result.foreach { x =>
      val tmp = x.getAs[DenseVector]("probability")
      println(tmp)
      val predict = x.getAs[String]("predictedLabel")
      println(predict)
      m = Map[String, Any]("falseProb" -> tmp(0), "trueProb" -> tmp(1), "prediction" -> predict)
    }
    println(m)

    (m)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    val obj = this.namedObjects.get("model:claimModel")
    val names = this.namedObjects.getNames()
    names.foreach(println)
    if (obj.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named object [model:claimModel]")
  }

}