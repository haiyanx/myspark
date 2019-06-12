package com.connectUtils

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.splunk.{Args, Service, ServiceArgs}
import org.apache.spark.{SparkConf, SparkContext}

object SearchFromSplunk {
  System.setProperty("https.protocols", "TLSv1.2,TLSv1.1,SSLv3")
  // Create a map of arguments and add login parameters
  var loginArgs = new ServiceArgs
  loginArgs.setUsername("admin")
  loginArgs.setPassword("vskysoft")
  loginArgs.setHost("172.16.10.94")
  loginArgs.setPort(8089)
  var service = Service.connect(loginArgs)

  /**
    * 通过传入的spl语句，返回csv格式的String
    * @param spl
    * @return
    */
  def connectSplunkAndReturn(spl: String): String = {
    val job = service.getJobs().create(spl)
    while (!job.isDone){
      try {
        Thread.sleep(500)
      } catch {
        case e: IOException => { e.printStackTrace }
      }
    }
    val outputArgs = new Args()
    outputArgs.put("count", "0")
    outputArgs.put("output_mode", "csv")
    val stream = job.getResults(outputArgs)
    inputStreamToString(stream)
  }


  def JsonSplunkAndReturn(spl: String)= {
    val job = service.getJobs().create(spl)
        while (!job.isDone){
          try {
            Thread.sleep(500)
          } catch {
            case e: IOException => { e.printStackTrace }
          }
        }
    val outputArgs = new Args()
    outputArgs.put("count", "0")
    outputArgs.put("output_mode", "json")
    val stream = job.getResults(outputArgs)

    val rd: BufferedReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"))
    val responseStrBuilder = new StringBuilder()
    try {
      var line = rd.readLine
      while (line != null) {
        responseStrBuilder.append(line + "\n")
        line = rd.readLine
      }
    } finally {
      rd.close
    }
    val str: String = responseStrBuilder.toString()
    val rowDate = JSON.parseObject(str)
    rowDate.getJSONArray("results")

  }

  /**
    * 将InputStream的数据转化为string类型输出
    * @param is
    * @return
    */
  def inputStreamToString(is: InputStream) = {
    val rd: BufferedReader = new BufferedReader(new InputStreamReader(is, "UTF-8"))
    val builder = new StringBuilder()
    try {
      var line = rd.readLine
      while (line != null) {
        builder.append(line + "\n")
        line = rd.readLine
      }
    } finally {
      rd.close
    }
    builder.toString
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

  }

}
