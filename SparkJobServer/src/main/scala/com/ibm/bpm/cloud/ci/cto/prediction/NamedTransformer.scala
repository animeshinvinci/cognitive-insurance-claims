package com.ibm.bpm.cloud.ci.cto.prediction

import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.storage.StorageLevel
import spark.jobserver.NamedObject
import spark.jobserver.NamedObjectPersister

case class NamedTransformer(transformer: Transformer, sc: SparkContext, storageLevel: StorageLevel) extends NamedObject

class TransformerPersister extends NamedObjectPersister[NamedTransformer] {

  override def persist(namedObj: NamedTransformer, name: String) {
    namedObj match {
      case NamedTransformer(transformer, sc, storageLevel) =>
        //these are not supported by DataFrame:
        //df.setName(name)
        //df.getStorageLevel match
        sc.parallelize(Seq(transformer), 1).persist(storageLevel)
    }
  }

  override def unpersist(namedObj: NamedTransformer) {
    namedObj match {
      case NamedTransformer(transformer, sc, _) =>
        sc.parallelize(Seq(transformer), 1).unpersist(blocking = false)
    }
  }
  /**
   * Calls df.persist(), which updates the DataFrame's cached timestamp, meaning it won't get
   * garbage collected by Spark for some time.
   * @param namedDF the NamedDataFrame to refresh
   */
  override def refresh(namedDF: NamedTransformer): NamedTransformer = namedDF match {
    case NamedTransformer(model, sc, storageLevel) =>
      sc.parallelize(Seq(model), 1).persist(storageLevel)
      namedDF
  }
}