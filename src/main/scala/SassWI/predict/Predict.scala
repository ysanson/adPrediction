package SassWI.predict

import SassWI.transformations.TransformDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession

object Predict{

  def predict(spark: SparkSession, dataPath: String): Unit = {

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .json(dataPath)

    val interests = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(("InterestTraduction.csv"))

    val data = TransformDataset.transform(df, interests)

    val model = LogisticRegressionModel.load("model").setPredictionCol("features")
    val predictions = model.transform(data.select("user", "features"))
    val result = data.join(predictions/*, data("user") == predictions("user")*/)
    result.show()
    result.write.csv("output.csv")
  }
}
