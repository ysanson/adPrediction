package SassWI

import org.apache.spark.sql
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors

import scala.annotation.tailrec
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

  @tailrec
  def allColsToLabels(df: sql.DataFrame, arr: Array[String]): sql.DataFrame= {
    if(arr.head.nonEmpty){
      allColsToLabels(stringToLabels(df, arr.head), arr.tail)
    }
    else df
  }

  def stringToLabels(df: sql.DataFrame, colName: String): sql.DataFrame ={
    val indexer = new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName+"Index")

    val indexed = indexer.fit(df).transform(df)
    indexed.show()
    indexed
  }


  //Columns must be numeric values
  def listToVector(df: sql.DataFrame): sql.DataFrame ={
    val assembler = new VectorAssembler()
      .setInputCols(Array("appOrSite","bidfloor","city","exchange","impid","label","media","network","os","publisher","size","type","user","newInterests"))
      .setOutputCol("vectorOutput")

    val output = assembler.transform(df)
    output.select("features").show(false)
    output
  }
}

