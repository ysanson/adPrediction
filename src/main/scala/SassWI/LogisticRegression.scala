package SassWI

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.Row

object LogisticRegression {

  def logisticRegressionMethod(df: sql.DataFrame): Unit = {
    //load data in RDD labelPoint
    //val data = MLUtils.loadLibSVMFile(new SparkContext( new SparkConf()), "path")
    //val data: RDD[Row] = df.select("features", "labelIndex").limit(20).rdd
    //transform row into labeledPoint
    //val d: RDD[LabeledPoint] = data.map((l:Row) => LabeledPoint(l.fieldIndex("labelIndex"), Vectors.dense(l.fieldIndex("features"))))

    val data = df.select("labelIndex", "features").withColumnRenamed("labelIndex", "label")
    // Split data into 2 datasets: training data and test data
    //val splits = d.randomSplit(Array(0.7, 0.3), seed = 11L)
    //val trainingData = splits(0).cache()
    //val testData = splits(1)

    // model creation
    val model = new LogisticRegression()
      .fit(data)

    // computation onto the test data
    /*val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }*/
    val predictions = model.transform(data)

    //add accuracy of the model
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      //.setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    // Save and load model

  }

}
