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
  /**
   * Transforms the OS column to lower case.
   * @param frame the dataframe
   * @return the dataframe
   */
  def EtlToLowerCase(frame: sql.DataFrame): sql.DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    frame.withColumn("os", lower($"os"))
  }

  /**
   * Transforms the interests as an array.
   * @param frame the original dataframe.
   * @return the new dataframe.
   */
  def interestsAsList(frame: sql.DataFrame): sql.DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    frame.withColumn("interests", split($"interests", ",").cast("array<String>"))
  }

  /**
   * Transforms all the interests codes to their label.
   * @param df the original dataframe
   * @param codesList the codes list with their labels.
   * @return the new dataframe.
   */
  def codeToInterest(df: sql.DataFrame, codesList: sql.DataFrame): sql.DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val codes: Map[String, String] = codesList.as[(String, String)].collect().toMap

    val transformList = udf((init: mutable.WrappedArray[String]) => {
      if (init == null) init
      else {
        init.map((code: String) => {
          if (!code.startsWith("IAB")) code.toLowerCase()
          else codes(code).toLowerCase()
        })
      }
    }).apply(col("interests"))

    df.withColumn("newInterests", array_distinct(transformList))
  }

  /**
   * Extracts the interests on single columns
   * @param df the original dataframe
   * @param interestsList the interests list
   * @return the new dataframe.
   */
  def explodeInterests(df: sql.DataFrame, interestsList: sql.DataFrame): sql.DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    @tailrec
    def internal(df: sql.DataFrame, interests: Seq[String]): sql.DataFrame = {
      if(interests.isEmpty) df
      else {
        val interest = interests.head
        val newDf = df
          .withColumn(interest, when($"newInterests".isNull, false)
            .otherwise(array_contains($"newInterests", interest)))
        internal(newDf, interests.tail)
      }
    }
    val interests: Seq[String] = interestsList.select("Interest").rdd.map(r => r(0).asInstanceOf[String].toLowerCase).collect()
    internal(df, interests).drop("interests", "newInterests")
  }

  @tailrec
  def colsToLabels(df: sql.DataFrame, arr: Array[String]): sql.DataFrame = {
    println(arr.head)
    if (arr.tail.length > 0) {
      val dfWithoutNull = filterNullValues(df, arr.head)
      colsToLabels(stringToLabels(dfWithoutNull, arr.head), arr.tail)
    }
    else filterNullValues(df, arr.head)
  }

  def filterNullValues(df: sql.DataFrame, colName: String): sql.DataFrame = {
    val array_ = udf(() => Array.empty[Int])

    if (colName == "interests" || colName == "size" || colName == "newInterests") df.withColumn(colName, coalesce(df.col(colName), array_()))
    else if (colName == "label") df.withColumn(colName, when(df.col(colName), 1).otherwise(0))
    else df.withColumn(colName, when(df.col(colName).isNull, 0).otherwise(df.col(colName)))
  }

  def stringToLabels(df: sql.DataFrame, colName: String): sql.DataFrame = {
    val indexer = new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "Index")

    if (colName == "interests" || colName == "size" || colName == "newInterests") df
    else {
      val indexed = indexer.fit(df).transform(df)
      indexed.show()
      indexed.drop(colName)
    }
  }


  //Columns must be numeric values
  def listToVector(df: sql.DataFrame): sql.DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("appOrSite", "bidfloor", "city", "exchange", "impid", "label", "media", "network", "os", "publisher", "size", "type", "user", "newInterests"))
      .setOutputCol("vectorOutput")

    val output = assembler.transform(df)
    output.select("features").show(false)
    output
  }
}

