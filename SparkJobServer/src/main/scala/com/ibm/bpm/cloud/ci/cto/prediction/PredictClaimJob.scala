package com.ibm.bpm.cloud.ci.cto.prediction

import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.SQLContext

import com.ibm.bpm.helpers.ClaimComplete
import com.typesafe.config.Config

import spark.jobserver.NamedObjectPersister
import spark.jobserver.NamedObjectSupport
import spark.jobserver.SparkJob
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.util.Failure
import scala.util.Success

object PredictClaimJob extends SparkJob with NamedObjectSupport {
  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def assemblerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    setupObjectStorage(sc)

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

    //TODO Need to check if we should load from object storage first
    val cvModel = Try(this.namedObjects.get[NamedModel]("model:claimModel").get.model.asInstanceOf[CrossValidatorModel])
      .getOrElse(this.namedObjects.get[NamedModel]("model:claimModel").get.model.asInstanceOf[PipelineModel])

    val assembler = this.namedObjects.get[NamedTransformer]("assembler:assembler").get.transformer.asInstanceOf[VectorAssembler]
    val labelIndexerModel = this.namedObjects.get[NamedModel]("indexer:labelIndexer").get.model.asInstanceOf[StringIndexerModel]
    val featureIndexerModel = this.namedObjects.get[NamedModel]("indexer:featureIndexer").get.model.asInstanceOf[VectorIndexerModel]

    val vectoredTable = assembler.transform(claimDF)
    val prediction = cvModel.transform(vectoredTable)
    prediction.show()

    var m: Map[String, Any] = Map()
    val result = getPredictionAndProbability(prediction) match {
      case Success(stuff) => {
        stuff.foreach { x =>
          val tmp = x.getAs[DenseVector]("probability")
          println(tmp)
          val predict = x.getAs[String]("predictedLabel")
          println(predict)
          m = Map[String, Any]("falseProb" -> tmp(0), "trueProb" -> tmp(1), "prediction" -> predict)
        }
      }
      case Failure(ex) => {
        val pred = prediction.select("predictedLabel").collect
        pred.foreach { x =>
          val p = x.getAs[String]("predictedLabel").toBoolean
          val pFalse = if (p) 0.0 else 1.0
          val pTrue = if (p) 1.0 else 0.0
          m = Map[String, Any]("falseProb" -> pFalse, "trueProb" -> pTrue, "prediction" -> p)
        }
      }
    }
    //
    //    result.foreach { x =>
    //      val tmp = x.getAs[DenseVector]("probability")
    //      println(tmp)
    //      val predict = x.getAs[String]("predictedLabel")
    //      println(predict)
    //      m = Map[String, Any]("falseProb" -> tmp(0), "trueProb" -> tmp(1), "prediction" -> predict)
    //    }
    println(m)

    (m)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    val obj = this.namedObjects.get("model:claimModel")
    val names = this.namedObjects.getNames()
    names.foreach(println)
    if (obj.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named object [model:claimModel]")
  }

  def getPredictionAndProbability(prediction: DataFrame): Try[Array[Row]] = for {
    predicts <- Try(prediction.select("probability", "predictedLabel").collect)
  } yield predicts

  def setupObjectStorage(sc: SparkContext) {
    val prefix = "fs.swift2d.service.keystone";
    val hconf = sc.hadoopConfiguration;
    hconf.set("fs.swift2d.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem");
    //the v2 urls used in all referenced docs are lies... damned lies.
    //See https://developer.ibm.com/answers/answers/270672/view.html
    hconf.set(prefix + ".auth.url", "https://identity.open.softlayer.com/v3/auth/tokens")
    hconf.set(prefix + ".auth.method", "keystoneV3")
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", "5b9d6598c966484baaf8ae45ef9a9bcf")
    hconf.set(prefix + ".username", "185edd37c1434a5ab12c8e3b3f9a7aa6")
    hconf.set(prefix + ".password", "GkWtD4GM^).60.qr")
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", "dallas")
    hconf.setBoolean(prefix + ".public", true)
    println("BU fs.swift2d.impl " + hconf.get("fs.swift2d.impl"))
    println("BU " + prefix + ".auth.url: " + sc.hadoopConfiguration.get(prefix + ".auth.url"))
    //Temporary workaround to get past "FileSystem not defined" 
    sc.textFile("swift2d://CogClaim.keystone/nshuklatest.txt").count
  }

}
