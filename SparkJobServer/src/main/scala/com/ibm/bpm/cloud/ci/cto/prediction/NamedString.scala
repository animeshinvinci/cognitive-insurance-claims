package com.ibm.bpm.cloud.ci.cto.prediction

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import spark.jobserver.NamedObject
import spark.jobserver.NamedObjectPersister

case class NamedString(string: String, sc: SparkContext, storageLevel: StorageLevel) extends NamedObject
class StringPersister extends NamedObjectPersister[NamedString] {
  override def persist(namedObj: NamedString, name: String) {
    namedObj match {
      case NamedString(assembler, sc, storageLevel) =>
        //these are not supported by DataFrame:
        //df.setName(name)
        //df.getStorageLevel match
        sc.parallelize(Seq(assembler), 1).persist(storageLevel)
    }
  }

  override def unpersist(namedObj: NamedString) {
    namedObj match {
      case NamedString(assembler, sc, _) =>
        sc.parallelize(Seq(assembler), 1).unpersist(blocking = false)
    }
  }
  /**
   * Calls df.persist(), which updates the DataFrame's cached timestamp, meaning it won't get
   * garbage collected by Spark for some time.
   * @param namedDF the NamedDataFrame to refresh
   */
  override def refresh(namedDF: NamedString): NamedString = namedDF match {
    case NamedString(model, sc, storageLevel) =>
      sc.parallelize(Seq(model), 1).persist(storageLevel)
      namedDF
  }
}