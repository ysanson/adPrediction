package SassWI.predict

import SassWI.transformations.TransformDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Predict{

  def predict(spark: SparkSession, dataPath: String): Unit = {
    import spark.implicits._
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .json(dataPath)
      .withColumn("id", monotonically_increasing_id)
      .drop("label")

    println("Number of records: " + df.count())

    val interests = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("InterestTraduction.csv")

    val data = TransformDataset.transform(df.drop("label"), interests)

    val model = LogisticRegressionModel.load("models/LogisticRegression").setPredictionCol("label").setFeaturesCol("features")
    val predictions = model
      .transform(data.select("features", "id"))
      .withColumn("label", when($"label" === 0.0, false ).otherwise(true))

    val result = df.join(predictions.select("id", "label"), "id")
    result.show()
    val resColumns: Array[String] = result.columns
    val reorder: Array[String] = resColumns.last +: resColumns.dropRight(1)

    val stringify = udf((vs: Seq[String]) => vs match {
      case null => null
      case _    => s"""[${vs.mkString(",")}]"""
    })

    result
      .select(reorder.head, reorder.tail: _*)
      .drop("id")
      .withColumn("size", stringify(col("size")))
      .repartition(1)
      .write.option("header", "true")
      .csv("output")
  }
}
