package SassWI.train

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}

object LogisticRegression {


  def calculateMetrics(predsAndLabels: RDD[(Double, Double)]): MulticlassMetrics = {
    val metrics = new MulticlassMetrics(predsAndLabels)
    val bMetrics = new BinaryClassificationMetrics(predsAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    //confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    //precision y labels (click or not click)
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
      println(s"Recall ($l) = " + metrics.recall(l))
    }

    bMetrics.roc
    // AUROC
    val auROC = bMetrics.areaUnderROC
    println("Area under ROC = " + auROC)

    metrics
  }

  def logisticRegressionMethod(df: sql.DataFrame): Unit = {
    val data: RDD[Row] = df.select("features", "labelIndex").rdd
    //transform row into labeledPoint
    val d: RDD[LabeledPoint] = data.map((l: Row) => LabeledPoint(l.getAs(1), Vectors.fromML(l.getAs(0)).toDense))

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

    calculateMetrics(predictionAndLabels)
    // Save and load model

  }

  def speedyLR(df: sql.DataFrame): LogisticRegressionModel = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = df
      .select("labelIndex", "features")
      .withColumnRenamed("labelIndex", "label")

    // Split data into 2 datasets: training data and test data
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0).cache()
    val testData = splits(1)

    // model creation
    val model = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(1500)
      .fit(trainingData)

    println(s"Coefficients: ${model.coefficients} \nIntercept: ${model.intercept}")

    val predictions: sql.DataFrame = model.transform(testData)
    val predictionsAndLabels: RDD[(Double, Double)] = predictions
      .select("prediction", "label")
      .as[(Double, Double)].rdd

    calculateMetrics(predictionsAndLabels)

    // Save and load model
    model
  }

}
