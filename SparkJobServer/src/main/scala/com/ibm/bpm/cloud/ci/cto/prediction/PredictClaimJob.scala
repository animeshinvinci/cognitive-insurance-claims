package com.ibm.bpm.cloud.ci.cto.prediction

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SQLContext
import org.scalactic.Bad
import org.scalactic.Every
import org.scalactic.Good
import org.scalactic.One
import org.scalactic.Or

import com.ibm.bpm.helpers.ClaimComplete
import com.typesafe.config.Config

import spark.jobserver.NamedObjectPersister
import spark.jobserver.NamedObjectSupport
import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValidation
import spark.jobserver.api.{ SparkJob => NewSparkJob, _ }
import spark.jobserver.api.JobEnvironment
import spark.jobserver.api.SingleProblem
import spark.jobserver.api.ValidationProblem
import org.apache.spark.ml.Pipeline
import scala.util.Try

object PredictClaimJob extends NewSparkJob with NamedObjectSupport {
  val MODEL_FILE = "swift2d://CogClaim.keystone/model.pipeline"
  val VECTOR_ASSEMBLER_FILE = "swift2d://CogClaim.keystone/vector_assembler.pipeline"
  val LABEL_INDEXER_FILE = "swift2d://CogClaim.keystone/label_indexer.pipeline"
  val VEC_INDEXER_MODEL_FILE = "swift2d://CogClaim.keystone/vec_index_model.pipeline"

  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def assemblerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister

  type JobOutput = collection.Map[String, Any]
  type JobData = Seq[Long]
  type cvModel = CrossValidatorModel

  override def runJob(sc: SparkContext, runtime: JobEnvironment, request: JobData): JobOutput = {
    val sqlContext = new SQLContext(sc)

    val claim = sc.parallelize(List(request))
    //println(claim)
    import sqlContext.implicits._
    val claimDF = claim.map(c => ClaimComplete(c(0), "", "", 0, "", c(2), c(1), "", "", "", "false")).toDF

    //claimDF.printSchema()
    //claimDF.show
    //sqlContext
    val cvModel = Try(runtime.namedObjects.get[NamedModel]("model:claimModel").get.model.asInstanceOf[CrossValidatorModel])
      .getOrElse(Pipeline.read.load(this.MODEL_FILE).asInstanceOf[CrossValidatorModel])
    //val cvModel = runtime.namedObjects.get[NamedModel]("model:claimModel").get.model.asInstanceOf[CrossValidatorModel]
    val assembler = Try(runtime.namedObjects.get[NamedTransformer]("assembler:assembler").get.transformer.asInstanceOf[VectorAssembler])
      .getOrElse(Pipeline.read.load(this.VECTOR_ASSEMBLER_FILE).asInstanceOf[VectorAssembler])
    val labelIndexerModel = Try(runtime.namedObjects.get[NamedModel]("indexer:labelIndexer").get.model.asInstanceOf[StringIndexerModel])
      .getOrElse(Pipeline.load(this.LABEL_INDEXER_FILE))
    val featureIndexerModel = Try(runtime.namedObjects.get[NamedModel]("indexer:featureIndexer").get.model.asInstanceOf[VectorIndexerModel])
      .getOrElse(Pipeline.read.load(this.VEC_INDEXER_MODEL_FILE))

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

    m
  }

  override def validate(sc: SparkContext, runtime: JobEnvironment, config: Config): JobData Or Every[ValidationProblem] = {
    setupObjectStorage(sc)
    val approvedAmount = config.getLong("input.approvedAmount")
    //println(approvedAmount)
    val estimate = config.getLong("input.estimate")
    //println(estimate)
    val creditScore = config.getLong("input.creditScore")
    //println(creditScore)
    val request = Seq(approvedAmount, estimate, creditScore)
    val obj = runtime.namedObjects.get("model:claimModel")
    val names = runtime.namedObjects.getNames()
    names.foreach(println)
    if (obj.isDefined) {
      Good(request)
    } else {
      //We don't have the model in memory.  Let's see if they exist in Object storage
      val model = Pipeline.read.load(this.MODEL_FILE)
      if (model.getStages.length <= 0) {
        Bad(One(SingleProblem(s"Missing named object [model:claimModel] or model not in object storage")))
      } else {
        Good(request)
      }
    }
  }

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
    //temp fix to get past some "file system not defined
    sc.textFile("swift2d://CogClaim.keystone/nshuklatest.txt").count
  }
}