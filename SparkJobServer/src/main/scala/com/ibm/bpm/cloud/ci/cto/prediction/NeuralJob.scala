package com.ibm.bpm.cloud.ci.cto.prediction

import java.nio.file.Files
import java.nio.file.Paths

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
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

class NeuralJob extends SparkJob with NamedObjectSupport {
  //This is an HDFS file location
  val PARQUET_FILE_CLAIMS = "/data/claims.parquet"

  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def assemblerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister
  implicit def floatPersister[T]: NamedObjectPersister[NamedDouble] = new DoublePersister
  implicit def dfPersister = new DataFramePersister

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)

    val eventTable = sqlContext.read.parquet("file:///Users/rtclauss/claims.parquet").cache
    eventTable.registerTempTable("claimData")
    val claimData = sqlContext.sql("select vehicleType, creditScore, estimate, approved, approvedAmount from claimData")
    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)
    //val vectoredTable = assembler.transform(vehicleTypeEncoded)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    val layers = Array[Int](3, 10, 2, 2)

    val mpc = new MultilayerPerceptronClassifier().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures").setBlockSize(128).setSeed(1234L) //.setMaxIter(100).setLayers(layers)
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, mpc, labelConverter))
    //val model = pipeline.fit(trainingData)

    //val predictions = model.transform(testData)
    //predictions.show()
    //predictions.select("predictedLabel", "approved", "features").show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    //val accuracy = evaluator.evaluate(predictions)
    //println("Test Error = " + (1.0 - accuracy))
    //val treeModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    //println("Learned classification tree model:\n" + treeModel.toDebugString)

    val paramGrid = new ParamGridBuilder().addGrid(mpc.maxIter, Array(100, 200, 300, 500)).addGrid(mpc.layers, Array(Array[Int](3, 2, 2), Array[Int](3, 5, 2), Array[Int](3, 2, 2, 2), Array[Int](3, 2, 2, 2))).build

    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(trainingData)
    //println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[MulticlassClassificationEvaluator].toDebugString)

    val cvPredict = cvModel.transform(testData)

    val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    //val cvAccuracy = cvEval.evaluate(predictions)
    val sameGuess = cvPredict.where("approved = predictedLabel").count
    val total = cvPredict.count.toDouble
    val cvAccuracy = sameGuess / total
    println(cvAccuracy)
    //cvPredict.registerTempTable("cvPredict")

    this.namedObjects.update("model:claimModel", NamedModel(cvModel, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("assembler:assembler", NamedTransformer(assembler, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:labelIndexer", NamedModel(labelIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:featureIndexer", NamedModel(featureIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("accuracy:accuracy", NamedDouble(cvAccuracy, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:homeStateIndexer", NamedModel(homeStateIndexer, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:vehicleTypeIndexer", NamedModel(vehicleTypeIndexer, sc, StorageLevel.MEMORY_ONLY))

    (1.0 - cvAccuracy)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    if (Files.exists(Paths.get(PARQUET_FILE_CLAIMS))) SparkJobValid else SparkJobInvalid(s"Missing claim parquet file")
  }
}