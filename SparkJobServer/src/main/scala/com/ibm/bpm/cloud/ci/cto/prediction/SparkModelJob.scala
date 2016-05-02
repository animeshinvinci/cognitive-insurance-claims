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

class SparkModelJob extends SparkJob with NamedObjectSupport {

  //This is an HDFS file location
  val PARQUET_FILE_CLAIMS = "/data/claims.parquet"

  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def assemblerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister
  implicit def floatPersister[T]: NamedObjectPersister[NamedDouble] = new DoublePersister
  implicit def dfPersister = new DataFramePersister

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)

    val eventTable = sqlContext.read.parquet(PARQUET_FILE_CLAIMS).cache
    eventTable.registerTempTable("claimData")
    val claimData = sqlContext.sql("select vehicleType, creditScore, estimate, approved, approvedAmount from claimData")
    //claimData.show()

    //    val homeStateIndexer = new StringIndexer().setInputCol("homeState").setOutputCol("homeStateIndex").fit(claimData)
    //    val homeStateIndexed = homeStateIndexer.transform(claimData)
    //
    //    val homeStateEncoder = new OneHotEncoder().setInputCol("homeStateIndex").setOutputCol("homeStateVec")
    //    val homeStateEncoded = homeStateEncoder.transform(homeStateIndexed)

    //    val vehicleTypeIndexer = new StringIndexer().setInputCol("vehicleType").setOutputCol("vehicleTypeIndex").fit(claimData)
    //    val vehicleTypeIndexed = vehicleTypeIndexer.transform(claimData)
    //
    //    val vehicleTypeEncoder = new OneHotEncoder().setInputCol("vehicleTypeIndex").setOutputCol("vehicleTypeVec")
    //    val vehicleTypeEncoded = vehicleTypeEncoder.transform(vehicleTypeIndexed)

    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)
    //val vectoredTable = assembler.transform(vehicleTypeEncoded)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    val dt = new RandomForestClassifier().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
    //val model = pipeline.fit(trainingData)

    //val predictions = model.transform(testData)
    //predictions.show()
    //predictions.select("predictedLabel", "approved", "features").show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")

    //val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(5, 10)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 5)).addGrid(dt.maxBins, Array(10, 50)).build
val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(1, 3)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 2)).addGrid(dt.maxBins, Array(2, 5)).build
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString)

    val cvPredict = cvModel.transform(testData)

    val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    //val cvAccuracy = cvEval.evaluate(predictions)
    val myeq = cvPredict.where("approved = predictedLabel").count
    val total = cvPredict.count.toDouble
    val cvAccuracy = myeq / total
    //cvPredict.registerTempTable("cvPredict")

    this.namedObjects.update("model:claimModel", NamedModel(cvModel, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("assembler:assembler", NamedTransformer(assembler, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:labelIndexer", NamedModel(labelIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:featureIndexer", NamedModel(featureIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("accuracy:accuracy", NamedDouble(cvAccuracy, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:homeStateIndexer", NamedModel(homeStateIndexer, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:vehicleTypeIndexer", NamedModel(vehicleTypeIndexer, sc, StorageLevel.MEMORY_ONLY))

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
    var m: Map[String, Any] = Map("bestModel" -> modelString, "accuracy" -> cvAccuracy*100.0)
    (m)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    if (Files.exists(Paths.get(PARQUET_FILE_CLAIMS))) SparkJobValid else SparkJobInvalid(s"Missing claim parquet file")
  }
}
