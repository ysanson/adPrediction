package SassWI

import org.apache.spark.sql
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.types.BooleanType

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
          .withColumn(interest, when($"newInterests".isNull, 0.0)
            .otherwise(
              when(array_contains($"newInterests", interest), 1.0)
                .otherwise(0.0)
            ))
        internal(newDf, interests.tail)
      }
    }
    val interests: Seq[String] = interestsList.select("Interest").rdd.map(r => r(0).asInstanceOf[String].toLowerCase).collect()
    internal(df, interests).drop("interests", "newInterests")
  }

  /**
   * Calls the method to convert all the string values to numeric values on all columns of the dataframe.
   * @param df the original dataframe
   * @param arr the array of column
   * @return the new dataframe
   */
  @tailrec
  def colsToLabels(df: sql.DataFrame, arr: Array[String]): sql.DataFrame = {
    println(arr.head)
    if (arr.tail.length > 0) {
      val dfWithoutNull = filterNullValues(df, arr.head)
      colsToLabels(stringToNumeric(dfWithoutNull, arr.head), arr.tail)
    }
    else filterNullValues(df, arr.head)
  }

  /**
   * Replace null value par 0.
   * @param df the original dataframe
   * @param colName column name
   * @return a new dataframe without null value
   */
  def filterNullValues(df: sql.DataFrame, colName: String): sql.DataFrame = {
    val array_ = udf(() => Array.empty[Int])
    val booleanCols = df.schema.fields.filter( x => x.dataType == BooleanType && x.name == colName)
    if (colName == "interests" || colName == "size") df.withColumn(colName, coalesce(df.col(colName), array_()))
    else if (booleanCols.length > 0) df.withColumn(colName, when(df.col(colName), 1).otherwise(0))
    else if (colName != "interests" && colName != "newInterests") df.withColumn(colName, when(df.col(colName).isNull, 0).otherwise(df.col(colName)))
    else df
  }

  /**
   * Converts all the string values to numeric values
   * @param df the original datafrale
   * @param colName the name of the column where the changes will be done
   * @return the new dataframe
   */
  def stringToNumeric(df: sql.DataFrame, colName: String): sql.DataFrame = {
    val indexer = new StringIndexer()
      .setInputCol(colName)
      .setOutputCol(colName + "Index")

    if (colName == "interests" || colName == "size" || colName == "newInterests") df
    else {
      val indexed = indexer.fit(df).transform(df)
      indexed.drop(colName)
    }
  }

  //Columns must be numeric values
  def listToVector(df: sql.DataFrame): sql.DataFrame = {
    //remove the size columns because it is always the same values and label because it is the column to predict
    val columns: Array[String] = df.columns.filter(c => c != "size" && c != "labelIndex" && c != "u.s.military" && c!= "u.s.governmentresources")
    val firstPart = columns.take(columns.length / 2)
    val secondPart = columns.drop(columns.length / 2)
    //columns.map(e => print("\"" + e + "\"," + " "))
    val assembler1 = new VectorAssembler()
      .setInputCols(firstPart)
      .setOutputCol("vector1")

    val assembler2 = new VectorAssembler()
      .setInputCols(secondPart)
      .setOutputCol("vector2")

    val output1 = assembler1.transform(df)
    val output2 = assembler2.transform(output1)

    val finalAssembler = new VectorAssembler()
      .setInputCols(Array("vector1", "vector2"))
      .setOutputCol("vectorOutput")


    val output = finalAssembler.transform(output2).drop("vector1", "vector2")
    output.select("vectorOutput").show(false)
    output
  }
}

