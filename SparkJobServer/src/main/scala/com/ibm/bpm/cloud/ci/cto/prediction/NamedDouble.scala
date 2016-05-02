package com.ibm.bpm.cloud.ci.cto.prediction

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import spark.jobserver.NamedObject
import spark.jobserver.NamedObjectPersister

case class NamedDouble(double: Double, sc: SparkContext, storageLevel: StorageLevel) extends NamedObject
class DoublePersister extends NamedObjectPersister[NamedDouble] {
  override def persist(namedObj: NamedDouble, name: String) {
    namedObj match {
      case NamedDouble(assembler, sc, storageLevel) =>
        //these are not supported by DataFrame:
        //df.setName(name)
        //df.getStorageLevel match
        sc.parallelize(Seq(assembler), 1).persist(storageLevel)
    }
  }

  override def unpersist(namedObj: NamedDouble) {
    namedObj match {
      case NamedDouble(assembler, sc, _) =>
        sc.parallelize(Seq(assembler), 1).unpersist(blocking = false)
    }
  }
  /**
   * Calls df.persist(), which updates the DataFrame's cached timestamp, meaning it won't get
   * garbage collected by Spark for some time.
   * @param namedDF the NamedDataFrame to refresh
   */
  override def refresh(namedDF: NamedDouble): NamedDouble = namedDF match {
    case NamedDouble(model, sc, storageLevel) =>
      sc.parallelize(Seq(model), 1).persist(storageLevel)
      namedDF
  }
}