package com.splunk.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据读取的文件内容，按照空格进行切分单词
  * 并对每个单词进行map操作（key,1）
  * 最后使用reduceByKey对key进行聚合统计，并打印
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")

    lines.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_ + _)
      .foreach(println)
  }
}
