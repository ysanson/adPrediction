package SassWI

import org.apache.spark.sql
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext

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

  def CodeToInterest(df: sql.DataFrame, codesList: sql.DataFrame) : sql.DataFrame = {
    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._
    val codes: Map[String, String] = codesList.as[(String, String)].collect().toMap

    val transformList = udf((init: mutable.WrappedArray[String]) => {
      if(init == null) init
      else {
        init.map((code: String) => {
          if(!code.startsWith("IAB")) code.toLowerCase()
          else codes(code).toLowerCase()
        })
      }
    }).apply(col("interests"))

    df.withColumn("newInterests", array_distinct(transformList))
  }

}

