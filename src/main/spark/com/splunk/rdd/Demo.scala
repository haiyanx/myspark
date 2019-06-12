package com.splunk.rdd

import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.connectUtils.SearchFromSplunk
import com.myUtils.TimeFormateUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.Map


object Demo {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
//    val hiveContext = new HiveContext(sc)

    /**
      * 获取index=ods_quality_perc的数据
      */
    val ods_quality_perc_spl = "search index=ods_quality_perc ProcessMachineID=5510 TubeID!=\"\" earliest=\"04/02/2019:08:00:00\" latest=\"04/02/2019:08:10:00\"" +
      "|table BoatID,BoatUseTimes,BoatUseTotalTimes,BorderValue,ColorStainTotalArea,ColorStainTotalCount,InterriorValue,MissingPartTotalArea," +
      "MissingPartTotalCount,PointInBoat,PointInPaw,ProcessMachineCode,ProcessMachineID,ProcessMachineName,TinyWhiteDotTotalCount,TestResult," +
      "Time,Track,TubeID,WaferID"
    val atmrdd: RDD[String] = sc.parallelize(SearchFromSplunk.connectSplunkAndReturn(ods_quality_perc_spl).split("\n"))

//    atmrdd.saveAsTextFile("E:\\vsky_splunk\\test111\\ods_quality_perc222")
    val QualityPercRdd: RDD[QualityPerc] = atmrdd.filter(!_.contains("BoatID")).map(t => {
      val s: Array[String] = t.split(",")
      QualityPerc(s(0),s(1),s(2),s(3),s(4),s(5),s(6),s(7),s(8),s(9),s(10),s(11),s(12),s(13),s(14),s(15),s(16),s(17),s(18),s(19))
    })
    // 创建DF
    // 需要导入隐式转换
    import sqlContext.implicits._
    val df: DataFrame = QualityPercRdd.toDF()
//    df.write.json("E:\\vsky_splunk\\test111\\ods_quality_perc")
//    df.write.json("E:\\vsky_splunk\\test111\\aa1")
    df.show()
    df.createTempView("quality_perc_atm")


    /**
      * 获取index=ods_process_perc的数据
      */

    val ods_process_perc_spl = "search index=ods_process_perc earliest=\"04/02/2019:00:00:00\" latest=\"04/02/2019:09:10:00\"  MessageType=Parameter "
    val results: JSONArray = SearchFromSplunk.JsonSplunkAndReturn(ods_process_perc_spl)
//    println(results)
//    println(results.size())
    import scala.collection.JavaConversions._
    // 创建可变的数组
    val mylist = mutable.MutableList[mutable.Map[String,String]]()
    val i = 0
    for (i <- 0 to results.size()-1) {
//      println(i)
      try{
        val value: JSONObject = JSON.parseObject(results.get(i).toString).getJSONObject("_raw")
//        println(value)
        var mymap: Map[String,String]= Map()
        val keys: Iterator[String] = value.keys.iterator
        while (keys.hasNext) {
          val key = keys.next()
          if(key.contains("Time")) mymap += ("Time" -> value.getString(key))
          if(key.contains("ID")) mymap += ("TubeID" -> value.getString(key))
          if(key.contains("Machineid")) mymap += ("Machineid" -> value.getString(key))
          if(key.contains("Memory_Text1")) mymap +=  ("Memory_Text1" ->  value.getString(key))
          if(key.contains("Gas_NH3_Volume")) mymap +=  ("Gas_NH3_Volume" ->  value.getString(key))
          if(key.contains("DataVar_All_Boat"))mymap += ("DataVar_All_Boat" ->  value.getString(key))
          if(key.contains("T_Spike_Cent_GZ_Actual")) mymap +=  ("T_Spike_Cent_GZ_Actual" ->  value.getString(key))
          if(key.contains("T_Paddle_Cent_GZ_Actual")) mymap += ("T_Paddle_Cent_GZ_Actual" ->  value.getString(key))
          if(key.contains("T_Paddle_Center_Actual")) mymap += ("T_Paddle_Center_Actual" ->  value.getString(key))
          if(key.contains("T_Spike_GasZone_Actual")) mymap += ("T_Spike_GasZone_Actual" ->  value.getString(key))
          if(key.contains("HFGen_All_PowerAct")) mymap += ("HFGen_All_PowerAct" ->  value.getString(key))
          if(key.contains("Vacuum_Tube_PressureAct")) mymap += ("Vacuum_Tube_PressureAct" ->  value.getString(key))
          if(key.contains("T_Paddle_GasZone_Actual")) mymap += ("T_Paddle_GasZone_Actual" ->  value.getString(key))
          if(key.contains("DataVar_All_Recipe")) mymap += ("DataVar_All_Recipe" ->  value.getString(key))
          if(key.contains("HFGen_All_Puls_Off")) mymap += ("HFGen_All_Puls_Off" ->  value.getString(key))
          if(key.contains("T_Spike_Center_Actual")) mymap += ("T_Spike_Center_Actual" ->  value.getString(key))
          if(key.contains("T_Paddle_LoadZone_Actual")) mymap += ("T_Paddle_LoadZone_Actual" ->  value.getString(key))
          if(key.contains("HFGen_All_Puls_On")) mymap += ("HFGen_All_Puls_On" ->  value.getString(key))
          if(key.contains("T_Spike_LoadZone_Actual")) mymap += ("T_Spike_LoadZone_Actual" ->  value.getString(key))
          if(key.contains("Gas_SiH4_Volume")) mymap += ("Gas_SiH4_Volume" ->  value.getString(key))
          if(key.contains("T_Paddle_Cent_LZ_Actual")) mymap += ("T_Paddle_Cent_LZ_Actual" ->  value.getString(key))
          if(key.contains("T_Spike_Cent_LZ_Actual")) mymap += ("T_Spike_Cent_LZ_Actual" ->  value.getString(key))

        }
        mylist += mymap
      } catch {
        case ex: Exception => println(ex)
      }


    }


    val rows: mutable.MutableList[Row] = mylist.map(t => {
      Row(t.get("Time"), t.get("TubeID"), t.get("Machineid"),t.get("Memory_Text1"),t.get("Gas_NH3_Volume"), t.get("DataVar_All_Boat"), t.get("T_Spike_Cent_GZ_Actual"), t.get("T_Paddle_Cent_GZ_Actual"),
        t.get("T_Paddle_Center_Actual"), t.get("T_Spike_GasZone_Actual"), t.get("HFGen_All_PowerAct"), t.get("Vacuum_Tube_PressureAct"), t.get("T_Paddle_GasZone_Actual"),
        t.get("DataVar_All_Recipe"), t.get("HFGen_All_Puls_Off"), t.get("T_Spike_Center_Actual"), t.get("T_Paddle_LoadZone_Actual"), t.get("HFGen_All_Puls_On"), t.get("T_Spike_LoadZone_Actual"),
        t.get("Gas_SiH4_Volume"), t.get("T_Paddle_Cent_LZ_Actual"), t.get("T_Spike_Cent_LZ_Actual"))
    })
    val structType: StructType = StructType(Array(
      StructField("Time", StringType, true),
      StructField("TubeID", StringType, true),
      StructField("Machineid",StringType,true),
      StructField("Memory_Text1", StringType, true),
      StructField("Gas_NH3_Volume", StringType, true),
      StructField("DataVar_All_Boat", StringType, true),
      StructField("T_Spike_Cent_GZ_Actual", StringType, true),
      StructField("T_Paddle_Cent_GZ_Actual", StringType, true),
      StructField("T_Paddle_Center_Actual", StringType, true),
      StructField("T_Spike_GasZone_Actual", StringType, true),
      StructField("HFGen_All_PowerAct", StringType, true),
      StructField("Vacuum_Tube_PressureAct", StringType, true),
      StructField("T_Paddle_GasZone_Actual", StringType, true),
      StructField("DataVar_All_Recipe", StringType, true),
      StructField("HFGen_All_Puls_Off", StringType, true),
      StructField("T_Spike_Center_Actual", StringType, true),
      StructField("T_Paddle_LoadZone_Actual", StringType, true),
      StructField("HFGen_All_Puls_On", StringType, true),
      StructField("T_Spike_LoadZone_Actual", StringType, true),
      StructField("Gas_SiH4_Volume", StringType, true),
      StructField("T_Paddle_Cent_LZ_Actual", StringType, true),
      StructField("T_Spike_Cent_LZ_Actual", StringType, true)
    ))
    val df3 = sqlContext.createDataFrame(rows,structType)
    df3.createTempView("t2")
//    df3.write.csv("E:\\vsky_splunk\\test111\\bb")
//    df3.write.json("E:\\vsky_splunk\\test111\\t2")
    df3.show()

    val view: RDD[String] = sc.textFile("E:\\vsky_splunk\\views.txt")
    val vewRdd: RDD[View] = view.map(t => {
      val strings: Array[String] = t.split("\t")
      View(strings(0),strings(1),strings(2))
    })
    // 需要导入隐式转换
    import sqlContext.implicits._
    val df2: DataFrame = vewRdd.toDF()
    df2.createTempView("ods_view")

    //    df2.write.json("E:\\vsky_splunk\\test111\\gg")
    val sql0 =
      """
        |SELECT ods_view.Machineid as Machineid, quality_perc_atm.BoatID as BoatID, quality_perc_atm.Time as Time,quality_perc_atm.TubeID as TubeID,
        |quality_perc_atm.ColorStainTotalArea as ColorStainTotalArea, quality_perc_atm.InterriorValue as InterriorValue, quality_perc_atm.MissingPartTotalArea as MissingPartTotalArea,
        |quality_perc_atm.MissingPartTotalCount as MissingPartTotalCount,quality_perc_atm.PointInBoat as PointInBoat,quality_perc_atm.TinyWhiteDotTotalCount as TinyWhiteDotTotalCount,
        |quality_perc_atm.TestResult as TestResult,quality_perc_atm.WaferID as WaferID
        |FROM quality_perc_atm ,ods_view
        |WHERE quality_perc_atm.ProcessMachineID = ods_view.Workid
      """.stripMargin
    val frame: DataFrame = sqlContext.sql(sql0)
    frame.createTempView("t1")
//    frame.write.json("E:\\vsky_splunk\\test111\\t1")
    frame.show()

    val sql1 =
      """
        |select t1.BoatID,t1.TubeID,t1.Machineid,t1.Time as a_time,t2.Time as b_time,t2.DataVar_All_Boat
        |FROM
        |t1,
        |t2
        |where t1.BoatID = t2.DataVar_All_Boat
        |and t1.TubeID = t2.TubeID
        |and t1.Machineid=t2.Machineid
        |and to_timestamp(t1.Time,"yyyy-MM-dd HH:mm:ss") > to_timestamp(t2.Time,"yyyy-MM-dd HH:mm:ss")
      """.stripMargin

    sqlContext.sql(sql1).show()
//    sqlContext.sql(sql1).write.csv("E:\\vsky_splunk\\test111\\tt")

  }
}

case class QualityPerc(BoatID:String,BoatUseTimes:String,BoatUseTotalTimes:String,BorderValue:String,
                       ColorStainTotalArea:String,ColorStainTotalCount:String,InterriorValue:String,MissingPartTotalArea:String,
                       MissingPartTotalCount:String,PointInBoat:String,PointInPaw:String,ProcessMachineCode:String,ProcessMachineID:String,
                       ProcessMachineName:String,TinyWhiteDotTotalCount:String,TestResult:String,Time:String,Track:String,TubeID:String,WaferID:String)

case class View(Workid:String,Num:String,Machineid:String)