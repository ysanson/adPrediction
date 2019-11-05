package SassWI.train

import SassWI.transformations.Etl._
import SassWI.transformations.TransformDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object TrainModel extends App {

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

    val data = TransformDataset.transform(df, etldf)

    //LogisticRegression.logisticRegressionMethod(df6)
    val model = LogisticRegression.speedyLR(data)
    model.save("model")
    //LogisticRegression.randomForestAlgorithm(df6)
    //MultilayerPerceptron.MultilayerPerceptronMethod(df6)
    //DecisionTrees.performCalculation(df6)

    spark.close()
  }

}
