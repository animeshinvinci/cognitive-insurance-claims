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
import com.ibm.bpm.helpers.LoanAppCompletion
import org.apache.spark.ml.feature.OneHotEncoder

class PredictLoanApproval extends SparkJob with NamedObjectSupport {
  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def assemblerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val input = jobConfig.getObject("input")
    val customerType = jobConfig.getString("input.customerType")
    //println(approvedAmount)
    val loanAmount = jobConfig.getLong("input.loanAmount")
    //println(estimate)
    val creditScore = jobConfig.getString("input.creditScore")
    //println(creditScore)
    val request = Seq(customerType, loanAmount, creditScore)
    val loan = sc.parallelize(List(request))
    //println(claim)
    //val claimDF = claim.map(c => ClaimComplete(c(0), "", "", 0, "", c(2), c(1), "", "", "", "false")).toDF

    val loanDF = loan.map(c => LoanAppCompletion("", "", c.lift(0).get.asInstanceOf[String], c.lift(1).get.asInstanceOf[Long], 0, c.lift(2).get.asInstanceOf[String], 0, "", "", 0)).toDF
    //claimDF.printSchema()
    //claimDF.show
    //sqlContext

    val cvModel = this.namedObjects.get[NamedModel]("model:loanModel").get.model.asInstanceOf[CrossValidatorModel]

    val assembler = this.namedObjects.get[NamedTransformer]("assembler:loanAssembler").get.transformer.asInstanceOf[VectorAssembler]
    val creditIndexer = this.namedObjects.get[NamedModel]("indexer:creditIndexer").get.model.asInstanceOf[StringIndexerModel]
    val ctypeIndexer = this.namedObjects.get[NamedModel]("indexer:ctypeIndexer").get.model.asInstanceOf[StringIndexerModel]
    val creditEncoder = this.namedObjects.get[NamedTransformer]("indexer:creditEncoder").get.transformer.asInstanceOf[OneHotEncoder]
    val ctypeEncoder = this.namedObjects.get[NamedTransformer]("indexer:ctypeEncoder").get.transformer.asInstanceOf[OneHotEncoder]
    val labelIndexerModel = this.namedObjects.get[NamedModel]("indexer:loanLabelIndexer").get.model.asInstanceOf[StringIndexerModel]
    val featureIndexerModel = this.namedObjects.get[NamedModel]("indexer:loanFeatureIndexer").get.model.asInstanceOf[VectorIndexerModel]
    loanDF.registerTempTable("loan")
    val loanData = sqlContext.sql("select customerType, loanAmount, creditScore from loan")
    loanData.show
    val creditIndexed = creditIndexer.transform(loanData)
    creditIndexed.show
    val creditEncoded = creditEncoder.transform(creditIndexed)
    creditEncoded.show
    val ctypeIndexed = ctypeIndexer.transform(creditEncoded)
    ctypeIndexed.show
    val ctypeEncoded = ctypeEncoder.transform(ctypeIndexed)
    ctypeEncoded.show

    val vectoredTable = assembler.transform(ctypeEncoded)
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
    val obj = this.namedObjects.get("model:loanModel")
    val names = this.namedObjects.getNames()
    names.foreach(println)
    if (obj.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named object [model:loanModel]")
  }

}