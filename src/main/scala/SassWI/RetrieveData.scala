package SassWI

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import SassWI.Etl._

object RetrieveData extends App {

  /**
   * Main function
   *
   * @param args arguments
   */
  override def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
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
      .json("data-students.json")

    // Read file for etl
    var etldf = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(("InterestTraduction.csv"))

    //df.printSchema()
    val df2 = interestsAsList(EtlToLowerCase(df))
    df2.show()
    df2.printSchema()
    val df3 = SassWI.Etl.CodeToInterest(df2, etldf)
    df3.printSchema()
    df3.select("interests").show()
    spark.close()
  }

}