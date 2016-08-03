package com.ibm.bpm.cloud.ci.cto.prediction

import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
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

import com.typesafe.config.Config

import spark.jobserver.DataFramePersister
import spark.jobserver.NamedObjectPersister
import spark.jobserver.NamedObjectSupport
import spark.jobserver.SparkJob
import spark.jobserver.SparkJobInvalid
import spark.jobserver.SparkJobValid
import spark.jobserver.SparkJobValidation
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

object SparkModelJob extends SparkJob with NamedObjectSupport {
  //Data files
  val PARQUET_FILE_CLAIMS = "swift2d://CogClaim.keystone/claims.parquet"
  //Model information
  val MODEL_TYPE = "swift2d://CogClaim.keystone/modelType.txt"
  val MODEL_FILE = "swift2d://CogClaim.keystone/model.pipeline"
  val VECTOR_ASSEMBLER_FILE = "swift2d://CogClaim.keystone/vector_assembler.pipeline"
  val LABEL_INDEXER_FILE = "swift2d://CogClaim.keystone/label_indexer.pipeline"
  val VEC_INDEXER_MODEL_FILE = "swift2d://CogClaim.keystone/vec_index_model.pipeline"

  implicit def modelPersister[T]: NamedObjectPersister[NamedModel] = new ModelPersister
  implicit def assemblerPersister[T]: NamedObjectPersister[NamedTransformer] = new TransformerPersister
  implicit def floatPersister[T]: NamedObjectPersister[NamedDouble] = new DoublePersister
  implicit def stringPersister[T]: NamedObjectPersister[NamedString] = new StringPersister
  implicit def dfPersister = new DataFramePersister

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    setupObjectStorage(sc)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val modelType = jobConfig.getString("input.modelType")

    val eventTable = sqlContext.read.parquet(PARQUET_FILE_CLAIMS).cache
    eventTable.registerTempTable("claimData")
    val claimData = sqlContext.sql("select vehicleType, creditScore, estimate, approved, approvedAmount from claimData")
    //claimData.show()
    val (cvModel, cvAccuracy) =
      modelType match {
        case "kmeans" => generateKMeans(modelType, sc, claimData)
        case "decisionForest" => generateRandomDecisionForest(modelType, sc, claimData)
        case "neuralNet" => generateMPC(modelType, sc, claimData)
        case "logReg" => generateLogisticRegression(modelType, sc, claimData)
        case "nBayes" => generateNaiveBayes(modelType, sc, claimData)
      }

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
    println("Best Model is: \n" + modelString)
    //modelString.to
    //var modelString = cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[LogisticRegressionModel]
    var m: Map[String, Any] = Map("bestModel" -> modelString, "accuracy" -> cvAccuracy * 100.0, "numClaims" -> eventTable.count())
    m
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    //if (Files.exists(Paths.get(PARQUET_FILE_CLAIMS))) SparkJobValid else SparkJobInvalid(s"Missing claim parquet file")
    Try(config.getString("input.modelType"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("Include one of modelType=[decisionForest, kmeans, neuralNet, nBayes, logReg] as a parameter"))
  }

  def generateRandomDecisionForest(modelType: String, sc: SparkContext, claimData: DataFrame): (CrossValidatorModel, Double) = {
    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    val dt = new RandomForestClassifier().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("approvedIndex") //.setRawPredictionCol("prediction") //.setMetricName("precision")
    val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(1, 4)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 4)).addGrid(dt.maxBins, Array(3, 4)).build
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(10)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString)

    val cvPredict = cvModel.transform(testData)

    //val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    //val cvAccuracy = cvEval.evaluate(predictions)
    val myeq = cvPredict.where("approved = predictedLabel").count
    val total = cvPredict.count.toDouble
    val cvAccuracy = myeq / total
    //cvPredict.registerTempTable("cvPredict")

    saveObjectsToMemory(modelType, cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc)
    println("objects saved to memory")
    //TODO once we move to Spark 2.0 uncomment the following line.
    //saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
  }

  def generateLogisticRegression(modelType: String, sc: SparkContext, claimData: DataFrame): (CrossValidatorModel, Double) = {
    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)
    //val vectoredTable = assembler.transform(vehicleTypeEncoded)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    val lr = new LogisticRegression().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures").setProbabilityCol("probability")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
    // val model = pipeline.fit(trainingData)
    // val predictions = model.transform(testData)
    // predictions.show()
    // predictions.select("predictedLabel", "approved", "features").show()

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("approvedIndex") //.setPredictionCol("prediction")
    val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(.1, .3, .5)).addGrid(lr.maxIter, Array(100, 200, 500)).addGrid(lr.elasticNetParam, Array(.5, .8, .9)).build()
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(10)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[LogisticRegressionModel])

    val cvPredict = cvModel.transform(testData)

    val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    val cvAccuracy = cvEval.evaluate(cvPredict)
    //cvPredict.registerTempTable("cvPredict")
    saveObjectsToMemory(modelType, cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc)
    //saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
  }

  def generateKMeans(modelType: String, sc: SparkContext, claimData: DataFrame): (PipelineModel, Double) = {
    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)
    //val vectoredTable = assembler.transform(vehicleTypeEncoded)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    // val kmeans = new LDA().setK(2).setFeaturesCol("indexedFeatures")
    val mpc = new KMeans().setK(2).setPredictionCol("prediction").setFeaturesCol("indexedFeatures").setMaxIter(500)
    //val dt = new RandomForestClassifier().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, mpc, labelConverter))
    val model = pipeline.fit(trainingData)

    print("cluster centers are: ")
    model.stages(2).asInstanceOf[KMeansModel].clusterCenters.foreach(println)
    val predictions = model.transform(testData)
    val wssse = model.stages(2).asInstanceOf[KMeansModel].computeCost(predictions)
    println("WSSE = " + wssse)
    saveObjectsToMemory(modelType, model, assembler, labelIndexer, featureIndexer, wssse, sc)
    //saveObjectsToObjectStorage(model, assembler, labelIndexer, featureIndexer)
    (model, wssse)
    //val predictions = model.transform(testData)
    //predictions.show()
    //predictions.select("predictedLabel", "approved", "features").show()

    //TODO once https://issues.apache.org/jira/browse/SPARK-16857 is addressed, uncomment
    /*
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex") //.setPredictionCol("prediction")
    val paramGrid = new ParamGridBuilder().addGrid(mpc.maxIter, Array(100, 200, 500)).build()
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(10)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[KMeansModel])
    println("cluster centers are: " + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[KMeansModel].clusterCenters)

    val cvPredict = cvModel.transform(testData)
    val cvEval = new BinaryClassificationEvaluator().setLabelCol("approvedIndex") //.setPredictionCol("prediction").setMetricName("precision")
    val cvAccuracy = cvEval.evaluate(cvPredict)
    saveObjectsToMemory(cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc)
    saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
    */

  }

  def generateNaiveBayes(modelType: String, sc: SparkContext, claimData: DataFrame): (CrossValidatorModel, Double) = {
    val assembler = new VectorAssembler().setInputCols(Array("approvedAmount", "estimate", "creditScore" /*,"homeStateVec","vehicleTypeVec"*/ )).setOutputCol("features")
    val vectoredTable = assembler.transform(claimData)
    //val vectoredTable = assembler.transform(vehicleTypeEncoded)

    val labelIndexer = new StringIndexer().setInputCol("approved").setOutputCol("approvedIndex").fit(vectoredTable)
    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(vectoredTable)

    val Array(trainingData, testData, trialData) = vectoredTable.randomSplit(Array(0.7, 0.2, 0.1))

    val nb = new NaiveBayes().setLabelCol("approvedIndex").setFeaturesCol("indexedFeatures").setProbabilityCol("probability")
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, nb, labelConverter))

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("approvedIndex") //.setPredictionCol("prediction")
    val paramGrid = new ParamGridBuilder().addGrid(nb.smoothing, Array(1, 1.5)).build()
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(10)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[NaiveBayesModel])
    cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[NaiveBayesModel].params.foreach(println)
    val cvPredict = cvModel.transform(testData)

    val cvEval = new BinaryClassificationEvaluator().setLabelCol("approvedIndex") //.setPredictionCol("prediction").setMetricName("precision")
    val cvAccuracy = cvEval.evaluate(cvPredict)
    //cvPredict.registerTempTable("cvPredict")

    saveObjectsToMemory(modelType, cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc)
    //saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
  }

  def generateMPC(modelType: String, sc: SparkContext, claimData: DataFrame): (CrossValidatorModel, Double) = {
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
    //val model = pipeline.fit(trainingData)

    //val predictions = model.transform(testData)
    //predictions.show()
    //predictions.select("predictedLabel", "approved", "features").show()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction")

    //val paramGrid = new ParamGridBuilder().addGrid(dt.numTrees, Array(5, 10)).addGrid(dt.impurity, Array("gini", "entropy")).addGrid(dt.maxDepth, Array(1, 5)).addGrid(dt.maxBins, Array(10, 50)).build
    val paramGrid = new ParamGridBuilder().addGrid(mpc.blockSize, Array(128, 256, 512)).addGrid(mpc.maxIter, Array(100, 200, 500)).build()
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator).setEstimatorParamMaps(paramGrid).setNumFolds(10)
    val cvModel = cv.fit(trainingData)
    println("Best Model is: \n" + cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[MultilayerPerceptronClassificationModel])

    val cvPredict = cvModel.transform(testData)

    val cvEval = new MulticlassClassificationEvaluator().setLabelCol("approvedIndex").setPredictionCol("prediction").setMetricName("precision")
    val cvAccuracy = cvEval.evaluate(cvPredict)
    //cvPredict.registerTempTable("cvPredict")

    saveObjectsToMemory(modelType, cvModel, assembler, labelIndexer, featureIndexer, cvAccuracy, sc)
    //saveObjectsToObjectStorage(cvModel, assembler, labelIndexer, featureIndexer)
    (cvModel, cvAccuracy)
  }

  def saveObjectsToMemory(modelType: String, cv: CrossValidatorModel, vecAssembler: VectorAssembler, labelIndexer: StringIndexerModel, vecIndexerModel: VectorIndexerModel, cvAccuracy: Double, sc: SparkContext) {
    this.namedObjects.update("model:modelType", NamedString(modelType, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("model:claimModel", NamedModel(cv, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("assembler:assembler", NamedTransformer(vecAssembler, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:labelIndexer", NamedModel(labelIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:featureIndexer", NamedModel(vecIndexerModel, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("accuracy:accuracy", NamedDouble(cvAccuracy, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:homeStateIndexer", NamedModel(homeStateIndexer, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:vehicleTypeIndexer", NamedModel(vehicleTypeIndexer, sc, StorageLevel.MEMORY_ONLY)
  }

  def saveObjectsToMemory(modelType: String, model: PipelineModel, vecAssembler: VectorAssembler, labelIndexer: StringIndexerModel, vecIndexerModel: VectorIndexerModel, cvAccuracy: Double, sc: SparkContext) {
    this.namedObjects.update("model:modelType", NamedString(modelType, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("model:claimModel", NamedModel(model, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("assembler:assembler", NamedTransformer(vecAssembler, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:labelIndexer", NamedModel(labelIndexer, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("indexer:featureIndexer", NamedModel(vecIndexerModel, sc, StorageLevel.MEMORY_ONLY))
    this.namedObjects.update("accuracy:accuracy", NamedDouble(cvAccuracy, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:homeStateIndexer", NamedModel(homeStateIndexer, sc, StorageLevel.MEMORY_ONLY))
    //this.namedObjects.update("model:vehicleTypeIndexer", NamedModel(vehicleTypeIndexer, sc, StorageLevel.MEMORY_ONLY)
  }

  def saveObjectsToObjectStorage(cv: CrossValidatorModel, vecAssembler: VectorAssembler, labelIndexer: StringIndexerModel, vecIndexerModel: VectorIndexerModel) {
    vecAssembler.write.overwrite().save(this.VECTOR_ASSEMBLER_FILE)
    labelIndexer.write.overwrite().save(this.LABEL_INDEXER_FILE)
    vecIndexerModel.write.overwrite().save(this.VEC_INDEXER_MODEL_FILE)
    cv.write.overwrite().save(this.MODEL_FILE)
  }

  def saveObjectsToObjectStorage(model: PipelineModel, vecAssembler: VectorAssembler, labelIndexer: StringIndexerModel, vecIndexerModel: VectorIndexerModel) {
    vecAssembler.write.overwrite().save(this.VECTOR_ASSEMBLER_FILE)
    labelIndexer.write.overwrite().save(this.LABEL_INDEXER_FILE)
    vecIndexerModel.write.overwrite().save(this.VEC_INDEXER_MODEL_FILE)
    model.write.overwrite().save(this.MODEL_FILE)
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
    //Temporary workaround to get past "FileSystem not defined" 
    sc.textFile("swift2d://CogClaim.keystone/nshuklatest.txt").count
  }
}
