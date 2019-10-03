package SassWI

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RetrieveData extends App {

  /**
   * Main function
   *
   * @param args arguments
   */
  override def main(args: Array[String]) {

    //Create a spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Sass")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //Read a file
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .json("\\data-students.json")

    df.show()
  }
}