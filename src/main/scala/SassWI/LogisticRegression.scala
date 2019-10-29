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
    val data: RDD[Row] = df.select("features", "labelIndex").rdd
    //transform row into labeledPoint
    val d: RDD[LabeledPoint] = data.map((l:Row) => LabeledPoint( l.getAs(1), Vectors.fromML(l.getAs(0)).toDense))

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
    println(s"Accuracy = $accuracy")

    //confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    //precision y labels (click or not click)
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Save and load model

  }

  def speedyLR(df: sql.DataFrame): Unit = {

    val data = df.select("labelIndex", "features").withColumnRenamed("labelIndex", "label")
    // Split data into 2 datasets: training data and test data
    val splits = data.randomSplit(Array(0.7, 0.3), seed = 11L)
    val trainingData = splits(0).cache()
    val testData = splits(1)

    // model creation
    val model = new LogisticRegression()
      //.setFamily("multinomial")
      .fit(trainingData)

    println(s"Coefficients: ${model.coefficients} \nIntercept: ${model.intercept}")
    //println(s"Multinomial coefficients: ${model.coefficientMatrix}")
    //println(s"Multinomial intercepts: ${model.interceptVector}")

    val predictions = model.transform(trainingData)

    //add accuracy of the model
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
    //.setMetricName("precision")

    val accuracy = evaluator.evaluate(predictions)

    println("Accuracy = " + accuracy)

    val trainingSummary = model.summary
    val objectiveHistory = trainingSummary.objectiveHistory
    println("objectiveHistory:")
    objectiveHistory.foreach(loss => println(loss))

    // Save and load model
    model

  }

}
