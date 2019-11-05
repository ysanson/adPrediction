package SassWI.main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import SassWI.train.TrainModel
import SassWI.predict.Predict

object Main extends App {
  override def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Create a spark Session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Sass")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    if(args.length == 0) Console.println("No args given, exiting.")
    else if(args(0) == "train") TrainModel.train(spark)
    else Predict.predict(spark, args(1))
  }
}
