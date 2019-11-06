package SassWI.train

import SassWI.transformations.TransformDataset
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object TrainModel {

  /**
   * Trains the model
   */
  def train(spark: SparkSession): Unit = {

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
    model.save("models/LogisticRegression")
    //LogisticRegression.randomForestAlgorithm(df6)
    //MultilayerPerceptron.MultilayerPerceptronMethod(df6)
    //DecisionTrees.performCalculation(df6)


  }

}
