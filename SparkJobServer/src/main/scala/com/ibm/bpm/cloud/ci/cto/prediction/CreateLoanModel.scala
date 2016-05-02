package com.ibm.bpm.cloud.ci.cto.prediction

import java.nio.file.Files
import java.nio.file.Paths

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

import com.typesafe.config.Config

import spark.jobserver.DataFramePersister
import spark.jobserver.NamedObjectPersister
import spark.jobserver.NamedObjectSupport
import spark.jobserver.SparkJob
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.OneHotEncoder

class CreateLoanModel extends SparkJob with NamedObjectSupport {

  //This is an HDFS file location
  val PARQUET_FILE_CLAIMS = "/data/loanapp.parquet"

  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def transformerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister
  implicit def floatPersister[T]: NamedObjectPersister[NamedDouble] = new DoublePersister
  implicit def dfPersister = new DataFramePersister

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)

    val eventTable = sqlContext.read.parquet("hdfs://sandbox:9000" + PARQUET_FILE_CLAIMS).cache
    eventTable.registerTempTable("loanData")

    val loanData = sqlContext.sql("select customerType, loanAmount, creditScore, status from loanData")
    loanData.show()
    println("creating credit indexer")
    val creditIndexer = new StringIndexer().setInputCol("creditScore").setOutputCol("creditScoreIndex").fit(loanData)
    val creditIndexed = creditIndexer.transform(loanData)
println("creating credit encoder")
    val creditEncoder = new OneHotEncoder().setInputCol("creditScoreIndex").setOutputCol("creditVec")
    val creditEncoded = creditEncoder.transform(creditIndexed)
println("creating customer type indexer")
    val ctypeIndexer = new StringIndexer().setInputCol("customerType").setOutputCol("customerTypeIndex").fit(creditEncoded)
    val ctypeIndexed = ctypeIndexer.transform(creditEncoded)
println("creating customer type encoder")
    val ctypeEncoder = new OneHotEncoder().setInputCol("customerTypeIndex").setOutputCol("ctypeVec")
    val ctypeEncoded = ctypeEncoder.transform(ctypeIndexed)
println("assembling")
    val assembler = new VectorAssembler().setInputCols(Array("loanAmount", "creditVec", "ctypeVec")).setOutputCol("features")
    val vectoredTable = assembler.transform(ctypeEncoded)
println("label indexer being built")
    val labelIndexer = new StringIndexer().setInputCol("status").setOutputCol("statusIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)
println("splitting data")
    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))
println("creating classifier")
    val dt = new RandomForestClassifier().setLabelCol("statusIndex").setFeaturesCol("indexedFeatures")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
println("pipeline created")
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("statusIndex").setPredictionCol("prediction").setMetricName("precision")
    val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(5, 10)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 10)).addGrid(dt.maxBins, Array(10, 300)).build
println("paramgrid created")
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
    println("cross validator created, beginning training")
    val cvModel = cv.fit(trainingData)
    println("training complete")

    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString)

    val cvPredict = cvModel.transform(testData)
    //val cvAccuracy = cvEval.evaluate(predictions)
    val equals = cvPredict.where("status = predictedLabel").count
    val total = cvPredict.count.toDouble
    val cvAccuracy = equals / total
    //cvPredict.registerTempTable("cvPredict")

    this.namedObjects.update("model:loanModel", NamedModel(cvModel, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("assembler:loanAssembler", NamedTransformer(assembler, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:creditIndexer", NamedModel(creditIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:ctypeIndexer", NamedModel(ctypeIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:creditEncoder", NamedTransformer(creditEncoder, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:ctypeEncoder", NamedTransformer(ctypeEncoder, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:loanLabelIndexer", NamedModel(labelIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:loanFeatureIndexer", NamedModel(featureIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("accuracy:loanAccuracy", NamedDouble(cvAccuracy, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:homeStateIndexer", NamedModel(homeStateIndexer, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:vehicleTypeIndexer", NamedModel(vehicleTypeIndexer, sc, StorageLevel.MEMORY_ONLY))
    println("Saved everything to shared context")
    var modelString = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString
    modelString = modelString.replaceAll("feature 0", "loanAmount")
    modelString = modelString.replaceAll("feature 1", "creditScore")
    modelString = modelString.replaceAll("feature 2", "customerType")
    modelString = modelString.replaceAll(" 0\\.0", " reject")
    modelString = modelString.replaceAll(" 1\\.0", " approve")
    modelString = modelString.replaceAll("\\n", "<br>")
    modelString = modelString.replaceAll("\\s", "&nbsp;")
    println("Best Model is: \n" + modelString)
    //modelString.to
    var m: Map[String, Any] = Map("bestModel" -> modelString, "accuracy" -> cvAccuracy * 100.0)
    (m)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    //if (Files.exists(Paths.get(PARQUET_FILE_CLAIMS))) SparkJobValid else SparkJobInvalid(s"Missing claim parquet file")
    SparkJobValid
  }

}