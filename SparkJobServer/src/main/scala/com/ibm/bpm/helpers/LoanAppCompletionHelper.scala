import org.apache.spark.sql.Row

object LoanAppCompletionHelper {
  type A[E] = collection.mutable.WrappedArray[E]
  implicit class RichRow(val r: Row) {
    def getOpt[T](n: String): Option[T] = {
      if (isNullAt(n)) {
        None
      } else {
        Some(r.getAs[T](n))
      }
    }

    def getStringOpt(n: String) = getOpt[String](n)
    def getString(n: String) = getStringOpt(n).get

    def getIntOpt(n: String) = getOpt[Int](n)
    def getInt(n: String) = r.getIntOpt(n).get

    def getLongOpt(n: String) = getOpt[Long](n)
    def getLong(n: String) = r.getLongOpt(n).get

    def getArray[T](n: String) = r.getAs[A[T]](n)

    def getRow(n: String) = r.getAs[Row](n)
    def getRows(n: String) = r.getAs[A[Row]](n)

    def isNullAt(n: String) = r.isNullAt(r.fieldIndex(n))
  }
}
