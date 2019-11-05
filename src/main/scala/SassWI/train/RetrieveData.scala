package SassWI.train

import SassWI.transformations.Etl._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

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
    def readInterests(): sql.DataFrame = {
      spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .option("inferSchema", "true")
        .csv(("InterestTraduction.csv"))
    }

    val etldf = readInterests()

    val df2 = df.transform(EtlToLowerCase)
      .transform(interestsAsList)
      .transform(codeToInterest(etldf))

    val df3 = colsToLabels(df2, df2.columns)
      .transform(explodeInterests(etldf))
      .transform(listToVector)


    //LogisticRegression.logisticRegressionMethod(df6)
    val model = LogisticRegression.speedyLR(df3)
    //LogisticRegression.randomForestAlgorithm(df6)
    //MultilayerPerceptron.MultilayerPerceptronMethod(df6)
    //DecisionTrees.performCalculation(df6)

    spark.close()
  }

}
