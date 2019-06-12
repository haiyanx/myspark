package com.splunk.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeSolution {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/airports.text")

    val airResult: RDD[(String, String)] = lines.filter(_.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")(6).toFloat > 40)
      .map(t => {
        val strings = t.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
        (strings(1), strings(6))
      })
    airResult.saveAsTextFile("D:/out/air.text")

  }

}
