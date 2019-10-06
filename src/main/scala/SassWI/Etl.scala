package SassWI

import org.apache.spark.sql
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object Etl {
  def EtlToLowerCase(frame: sql.DataFrame): sql.DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    frame.withColumn("os", lower($"os"))
  }

  def interestsAsList(frame: sql.DataFrame): sql.DataFrame = {
    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._
    frame.withColumn("interests", split($"interests", ",").cast("array<String>"))
  }

  def CodeToInterest(df: sql.DataFrame, etldf: sql.DataFrame) :sql.DataFrame = {
    val spark =  SparkSession.builder().getOrCreate()
    import spark.implicits._

    val transformList = (init:  Array[String]) => {
      if(init == null) return null
      init.map(code => {
        println(code)
        etldf.filter($"Code" === code).first().getAs[String]("Interests")
      })
    }

    val etl = udf(transformList)
    val testList = df.filter(not(isnull($"interests"))).first().getAs[mutable.WrappedArray[String]]("interests").toArray
    testList.map(item => println(item))
    println(transformList(testList))
    df.withColumn("interests", etl('interests))

    /*
    val row = interestList.map((line: sql.Row) => {
      println(line)
      line.toSeq.iterator.map( code => {
        println(code)
        dico.map(l => if (l.get(0) == code) {
          l.get(1)
        })
      }).toList
    })*/
    /*result.map(l => {
      l.map(line => line.map(e => print(e) ))
    })*/
    //etldf
  }

}

