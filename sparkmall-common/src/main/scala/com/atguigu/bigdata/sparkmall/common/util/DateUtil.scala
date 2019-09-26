package com.atguigu.bigdata.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {
    //给时间戳,返回字符串
  def formatStringByTimestamp(ts: Long, f: String = "yyyy-MM-dd HH:mm:ss"): String = {

    formatStringByDate(new Date(ts), f)
  }


  def formatStringByDate(d: Date, f: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val format = new SimpleDateFormat(f)


    format.format(d)
  }

  def parseLongByString( s : String, f:String = "yyyy-MM-dd HH:mm:ss" ) : Long = {
    parseDateByString(s, f).getTime
  }

  def parseDateByString(s : String, f:String = "yyyy-MM-dd HH:mm:ss"): Date = {
    val sdf = new SimpleDateFormat(f)
    sdf.parse(s)
  }


}
