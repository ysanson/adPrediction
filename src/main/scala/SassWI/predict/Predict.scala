package SassWI.predict

import SassWI.transformations.TransformDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession

object Predict extends App{
  override def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Sass")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .json(args(0))

    val interests = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(("InterestTraduction.csv"))

    val data = TransformDataset.transform(df, interests)

    val model = LogisticRegressionModel.load("model").setPredictionCol("features")
    val predictions = model.transform(data.select("user", "features"))
    val result = data.join(predictions/*, data("user") == predictions("user")*/)
  }
}
