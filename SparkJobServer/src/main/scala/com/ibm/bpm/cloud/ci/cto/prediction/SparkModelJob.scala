package com.ibm.bpm.cloud.ci.cto.prediction

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.scalactic._

import com.typesafe.config.Config

import spark.jobserver.DataFramePersister
import spark.jobserver.NamedObjectPersister
import spark.jobserver.NamedObjectSupport
//import spark.jobserver.SparkJob
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import spark.jobserver.api.{ SparkJob => NewSparkJob, _ }
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.clustering.KMeans

object SparkModelJob extends NewSparkJob with NamedObjectSupport {
  type JobOutput = Map[String, Any]
  type JobData = String
  //This is an HDFS file location
  val PARQUET_FILE_CLAIMS = "swift2d://CogClaim.keystone/claims.parquet"
  val MODEL_FILE = "swift2d://CogClaim.keystone/model.pipeline"
  val VECTOR_ASSEMBLER_FILE = "swift2d://CogClaim.keystone/vector_assembler.pipeline"
  val LABEL_INDEXER_FILE = "swift2d://CogClaim.keystone/label_indexer.pipeline"
  val VEC_INDEXER_MODEL_FILE = "swift2d://CogClaim.keystone/vec_index_model.pipeline"

  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def assemblerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister
  implicit def floatPersister[T]: NamedObjectPersister[NamedDouble] = new DoublePersister
  implicit def dfPersister = new DataFramePersister

  override def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput = {
    setupObjectStorage(sc)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.warehouse.dir", "file:///database")
    val eventTable = sqlContext.read.parquet(PARQUET_FILE_CLAIMS).cache
    eventTable.registerTempTable("claimData")
    val claimData = sqlContext.sql("select vehicleType, creditScore, estimate, approved, approvedAmount from claimData")
    //claimData.show()

    val (cvModel, cvAccuracy) = generateRandomDecisionForest(sc, claimData, runtime)

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
    var m: Map[String, Any] = Map("bestModel" -> modelString, "accuracy" -> cvAccuracy * 100.0, "numClaims" -> eventTable.count())
    m
  }

  override def validate(sc: SparkContext, runtime: JobEnvironment, config: Config): JobData Or Every[ValidationProblem] = {
    //if (Files.exists(Paths.get(PARQUET_FILE_CLAIMS))) SparkJobValid else SparkJobInvalid(s"Missing claim parquet file")
    Good("null")
  }

  def generateRandomDecisionForest(sc: SparkContext, claimData: DataFrame, runtime: JobEnvironment): (CrossValidatorModel, Double) = {

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

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction") //.setMetricName("precision")

    //val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(5, 10)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 5)).addGrid(dt.maxBins, Array(10, 50)).build
    val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(1, 4)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 4)).addGrid(dt.maxBins, Array(2, 5)).build
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString)

    val cvPredict = cvModel.transform(testData)

    //val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    //val cvAccuracy = cvEval.evaluate(predictions)
    val myeq = cvPredict.where("approved = predictedLabel").count
    val total = cvPredict.count.toDouble
    val cvAccuracy = myeq / total
    //cvPredict.registerTempTable("cvPredict")

    //    this.namedObjects.update("model:claimModel", NamedModel(cvModel, sc, StorageLevel.MEMORY_ONLY))
    //    this.namedObjects.update("assembler:assembler", NamedTransformer(assembler, sc, StorageLevel.MEMORY_ONLY))
    //    this.namedObjects.update("indexer:labelIndexer", NamedModel(labelIndexer, sc, StorageLevel.MEMORY_ONLY))
    //    this.namedObjects.update("indexer:featureIndexer", NamedModel(featureIndexer, sc, StorageLevel.MEMORY_ONLY))
    //    this.namedObjects.update("accuracy:accuracy", NamedDouble(cvAccuracy, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:homeStateIndexer", NamedModel(homeStateIndexer, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:vehicleTypeIndexer", NamedModel(vehicleTypeIndexer, sc, StorageLevel.MEMORY_ONLY))
    saveObjectsToMemory(cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc, runtime)
    println("objects saved to memory")
    saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
  }

  def generateLogisticRegression(sc: SparkContext, claimData: DataFrame, runtime: JobEnvironment): (CrossValidatorModel, Double) = {
    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)
    //val vectoredTable = assembler.transform(vehicleTypeEncoded)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    // val kmeans = new LDA().setK(2).setFeaturesCol("indexedFeatures")
    val mpc = new LogisticRegression().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures")
    //val dt = new RandomForestClassifier().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, mpc, labelConverter))
    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)
    //predictions.show()
    //predictions.select("predictedLabel", "approved", "features").show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction")

    //val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(5, 10)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 5)).addGrid(dt.maxBins, Array(10, 50)).build
    val paramGrid = new ParamGridBuilder().addGrid(mpc.regParam, Array(.1, .3, .5)).addGrid(mpc.maxIter, Array(100, 200, 500)).addGrid(mpc.elasticNetParam, Array(.5, .8, .9)).build()
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[LogisticRegressionModel])

    val cvPredict = cvModel.transform(testData)

    val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    val cvAccuracy = cvEval.evaluate(cvPredict)
    //cvPredict.registerTempTable("cvPredict")
    saveObjectsToMemory(cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc, runtime)
    saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
  }

  def generateKMeans(sc: SparkContext, claimData: DataFrame, runtime: JobEnvironment): (CrossValidatorModel, Double) = {
    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)
    //val vectoredTable = assembler.transform(vehicleTypeEncoded)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    // val kmeans = new LDA().setK(2).setFeaturesCol("indexedFeatures")
    val mpc = new KMeans().setK(2).setPredictionCol("prediction").setFeaturesCol("indexedFeatures")
    //val dt = new RandomForestClassifier().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, mpc, labelConverter))
    //val model = pipeline.fit(trainingData)

    //val predictions = model.transform(testData)
    //predictions.show()
    //predictions.select("predictedLabel", "approved", "features").show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction")

    //val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(5, 10)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 5)).addGrid(dt.maxBins, Array(10, 50)).build
    val paramGrid = new ParamGridBuilder().addGrid(mpc.maxIter, Array(100, 200, 500)).build()
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[LogisticRegressionModel])

    val cvPredict = cvModel.transform(testData)

    val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    val cvAccuracy = cvEval.evaluate(cvPredict)
    //cvPredict.registerTempTable("cvPredict")
    saveObjectsToMemory(cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc, runtime)
    saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
  }

  def generateMPC(sc: SparkContext, claimData: DataFrame, runtime: JobEnvironment): (CrossValidatorModel, Double) = {
    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)
    //val vectoredTable = assembler.transform(vehicleTypeEncoded)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    // val kmeans = new LDA().setK(2).setFeaturesCol("indexedFeatures")
    val mpc = new MultilayerPerceptronClassifier().setLayers(Array[Int](3, 3, 2)).setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures")
    //val dt = new RandomForestClassifier().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, mpc, labelConverter))
    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)
    //predictions.show()
    //predictions.select("predictedLabel", "approved", "features").show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction")

    //val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(5, 10)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 5)).addGrid(dt.maxBins, Array(10, 50)).build
    val paramGrid = new ParamGridBuilder().addGrid(mpc.blockSize, Array(128, 256, 512)).addGrid(mpc.maxIter, Array(100, 200, 500)).build()
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(3)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[MultilayerPerceptronClassificationModel])

    val cvPredict = cvModel.transform(testData)

    val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    val cvAccuracy = cvEval.evaluate(cvPredict)
    //cvPredict.registerTempTable("cvPredict")

    //    this.namedObjects.update("model:claimModel", NamedModel(cvModel, sc, StorageLevel.MEMORY_ONLY))
    //    this.namedObjects.update("assembler:assembler", NamedTransformer(assembler, sc, StorageLevel.MEMORY_ONLY))
    //    this.namedObjects.update("indexer:labelIndexer", NamedModel(labelIndexer, sc, StorageLevel.MEMORY_ONLY))
    //    this.namedObjects.update("indexer:featureIndexer", NamedModel(featureIndexer, sc, StorageLevel.MEMORY_ONLY))
    //    this.namedObjects.update("accuracy:accuracy", NamedDouble(cvAccuracy, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:homeStateIndexer", NamedModel(homeStateIndexer, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:vehicleTypeIndexer", NamedModel(vehicleTypeIndexer, sc, StorageLevel.MEMORY_ONLY))
    saveObjectsToMemory(cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc, runtime)
    saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
  }

  def saveObjectsToMemory(cv: CrossValidatorModel, vecAssembler: VectorAssembler, labelIndexer: StringIndexerModel, vecIndexerModel: VectorIndexerModel, cvAccuracy: Double, sc: SparkContext, runtime: JobEnvironment) {
    runtime.namedObjects.update("model:claimModel", NamedModel(cv, sc, StorageLevel.MEMORY_ONLY))
    runtime.namedObjects.update("assembler:assembler", NamedTransformer(vecAssembler, sc, StorageLevel.MEMORY_ONLY))
    runtime.namedObjects.update("indexer:labelIndexer", NamedModel(labelIndexer, sc, StorageLevel.MEMORY_ONLY))
    runtime.namedObjects.update("indexer:featureIndexer", NamedModel(vecIndexerModel, sc, StorageLevel.MEMORY_ONLY))
    runtime.namedObjects.update("accuracy:accuracy", NamedDouble(cvAccuracy, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:homeStateIndexer", NamedModel(homeStateIndexer, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:vehicleTypeIndexer", NamedModel(vehicleTypeIndexer, sc, StorageLevel.MEMORY_ONLY)
  }

  def saveObjectsToObjectStorage(cv: CrossValidatorModel, vecAssembler: VectorAssembler, labelIndexer: StringIndexerModel, vecIndexerModel: VectorIndexerModel) {
    vecAssembler.write.overwrite().save(this.VECTOR_ASSEMBLER_FILE)
    labelIndexer.write.overwrite().save(this.LABEL_INDEXER_FILE)
    vecIndexerModel.write.overwrite().save(this.VEC_INDEXER_MODEL_FILE)
    cv.write.overwrite().save(this.MODEL_FILE)
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
