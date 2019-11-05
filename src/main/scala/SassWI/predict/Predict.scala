package SassWI.predict

import SassWI.transformations.TransformDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Predict{

  def predict(spark: SparkSession, dataPath: String): Unit = {
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .json(dataPath)
      .withColumn("id", monotonically_increasing_id)

    df.show()
    println("Number of records: " + df.count())

    val interests = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("InterestTraduction.csv")

    val data = TransformDataset.transform(df, interests)


    data.show()

    val model = LogisticRegressionModel.load("model").setPredictionCol("prediction").setFeaturesCol("features")
    val predictions = model.transform(data.select("features", "id"))
    val result = df
      .join(predictions.select("id", "prediction", "probability"), "id")
    result.show()

    println("Number of rows: " + result.count())

    val stringify = udf((vs: Seq[String]) => vs match {
      case null => null
      case _    => s"""[${vs.mkString(",")}]"""
    })

    result
      .drop("probability")
      .withColumn("size", stringify(col("size")))
      .write.csv("output.csv")
  }
}
