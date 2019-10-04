package SassWI

import org.apache.spark.sql
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lower

object Etl {
  def EtlToLowerCase(frame: sql.DataFrame): sql.DataFrame = {
    frame = frame.withColumn("os",lower(col("os")))
    frame.show()
    return frame
  }
}

