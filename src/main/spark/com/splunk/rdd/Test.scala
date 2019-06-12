package com.splunk.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val view: RDD[String] = sc.textFile("E:\\vsky_splunk\\views.txt")
    val unit: RDD[View] = view.map(t => {
      val strings: Array[String] = t.split("\t")
      View(strings(0),strings(1),strings(2))
    })
    // 需要导入隐式转换
    import sqlContext.implicits._
    val df: DataFrame = unit.toDF()
    df.show()
  }
  case class View(Workid:String,Num:String,Machineid:String)
}
