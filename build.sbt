name := "adPrediction"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies := Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "2.4.4",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
  "org.apache.spark" %% "spark-mllib" % "2.4.4",
 // "com.github.fommil.netlib" %% "all" % "1.1.2"
)
