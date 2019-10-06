package SassWI

import org.apache.spark.sql
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object Etl {
  def EtlToLowerCase(frame: sql.DataFrame): sql.DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    frame.withColumn("os", lower($"os"))
  }

  def interestsAsList(frame: sql.DataFrame): sql.DataFrame = {
    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._
    frame.withColumn("interests", split($"interests", ",").cast("array<String>"))
  }

  def CodeToInterest(df: sql.DataFrame, etldf: sql.DataFrame) : sql.DataFrame = {
    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._

    val transformList = (init:  List[String]) => {
      if(init == null) return null
      else init.map(code => {
       println(code)
        if(!code.startsWith("IAB")) code
        else {
          val tmp = etldf.filter($"code" === code)
          tmp.show()
          tmp
            .first()
            .getAs[String]("Interest")
        }
      })
    }

    val etl = udf(transformList)
    etldf.show()
    val testList : List[String] = df.filter(not(isnull($"interests")))
      .first()
      .getAs[mutable.WrappedArray[String]]("interests")
      .toList
      .filter(!_.isEmpty)
    println(transformList(testList))
    df.withColumn("interests", etl(col("interests")))
  }

}

