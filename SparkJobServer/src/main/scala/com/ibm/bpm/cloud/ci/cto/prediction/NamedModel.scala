package com.ibm.bpm.cloud.ci.cto.prediction

import org.apache.spark.SparkContext
import org.apache.spark.ml.Model
import org.apache.spark.storage.StorageLevel

import spark.jobserver.NamedObject
import spark.jobserver.NamedObjectPersister

case class NamedModel(model: Model[X] forSome { type X <: Model[X] }, sc: SparkContext, storageLevel: StorageLevel) extends NamedObject

class ModelPersister extends NamedObjectPersister[NamedModel] {

  override def persist(namedObj: NamedModel, name: String) {
    namedObj match {
      case NamedModel(model, sc, storageLevel) =>
        //these are not supported by DataFrame:
        //df.setName(name)
        //df.getStorageLevel match
        sc.parallelize(Seq(model), 1).persist(storageLevel)
    }
  }

  override def unpersist(namedObj: NamedModel) {
    namedObj match {
      case NamedModel(model, sc, _) =>
        sc.parallelize(Seq(model), 1).unpersist(blocking = false)
    }
  }
  /**
   * Calls df.persist(), which updates the DataFrame's cached timestamp, meaning it won't get
   * garbage collected by Spark for some time.
   * @param namedDF the NamedDataFrame to refresh
   */
  override def refresh(namedDF: NamedModel): NamedModel = namedDF match {
    case NamedModel(model, sc, storageLevel) =>
      sc.parallelize(Seq(model), 1).persist(storageLevel)
      namedDF
  }
}
