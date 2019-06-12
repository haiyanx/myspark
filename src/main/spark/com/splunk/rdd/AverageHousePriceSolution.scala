package com.splunk.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("in/RealEstate.csv")

    lines.filter((line) => !line.contains("Bedrooms"))
      .map(t=>{
        val strings = t.split(",")
        (strings(3),strings(2))
      })
  }

}
