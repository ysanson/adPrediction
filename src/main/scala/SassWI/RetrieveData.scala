package SassWI

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import SassWI.Etl

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

    import spark.implicits._

    //Read a file
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .json("data-students.json")

    df.printSchema()
    val df2 = SassWI.Etl.EtlToLowerCase(df)
    df2.show()

    spark.close()
  }

}