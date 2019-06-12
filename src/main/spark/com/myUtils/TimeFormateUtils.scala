package com.myUtils

import java.text.SimpleDateFormat
import java.util.Date

object TimeFormateUtils {
  /**
    * 日期格式化函数，将数据中如期格式为"yyyy-MM-dd H:mm:ss"转化为"MM/dd/yyyy:H:mm:ss"
    * @param time 传入的字符串日期
    */
  def formatData(time:String)={
    val formatter  = new SimpleDateFormat("yyyy-MM-dd H:mm:ss")
    val formatter2  = new SimpleDateFormat("MM/dd/yyyy:H:mm:ss")
    formatter2.format(formatter.parse(time))

  }

  def stringFormateDate(time:String): Date = {
    val date: Date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time)
    date
  }

  def main(args: Array[String]): Unit = {
//    println(formatData("2019-04-01 23:48:05"))

    println(stringFormateDate("2019-04-01 23:48:05"))

  }

}
