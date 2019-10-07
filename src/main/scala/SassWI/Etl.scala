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

  def CodeToInterest(df: sql.DataFrame, codesList: sql.DataFrame) : sql.DataFrame = {
    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._

    val transformList = udf((init: Array[String]) => {
      if(init == null) return null
      else init.map((code: String) => {
        if(!code.startsWith("IAB")) code
        else codesList.filter($"Code" === code)
            .first()
            .getAs[String]("Interest")
      })
    }).apply(col("interests"))

    df.withColumn("newInterests", transformList)
  }

}

