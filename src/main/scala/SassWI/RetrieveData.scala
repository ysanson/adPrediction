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
    val etldf = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(("InterestTraduction.csv"))

    //df.printSchema()
    val df2 = interestsAsList(EtlToLowerCase(df))
   // df2.show()
    //df2.printSchema()
    val df3 = codeToInterest(df2, etldf)
   // df3.show()
    val df4 = colsToLabels(df3, df3.columns)
    val df5 = explodeInterests(df4, etldf)
    df5.show()
    val df6 = listToVector(df5)
    df6.show()
    spark.close()
  }

}