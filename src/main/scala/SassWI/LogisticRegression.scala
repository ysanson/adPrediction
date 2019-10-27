package SassWI

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
    val data: RDD[Row] = df.select("VectorOutput").limit(20).rdd
    val d = data.map((l:Row) => LabeledPoint(l.getAs(6), Vectors.dense(l.getDouble(0))))

    // Split data into 2 datasets: training data and test data
    val splits = d.randomSplit(Array(0.7, 0.3), seed = 11L)
    val trainingData = splits(0).cache()
    val testData = splits(1)

    // model creation
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(trainingData)

    // computation onto the test data
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    //add accuracy of the model
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println("Model Accuracy =" + accuracy)

    // Save and load model

  }

}
