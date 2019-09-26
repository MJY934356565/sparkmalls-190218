package com.atguigu.bigdata.sparkmall.common.util

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}

//配置工具类
object ConfigUtil {

  //专门用来读配置文件的
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")
  private val condBundle: ResourceBundle = ResourceBundle.getBundle("condition")


  def main(args: Array[String]): Unit = {
//   println(getValueByKey("hive.database"))
 println(getValueByJsonKey("startDate"))

  }


  /**
    * 从条件中获取数据
    *
    * @param jsonKey
    * @return
    */
  def getValueByJsonKey(jsonKey: String): String = {
    val jsonString: String = condBundle.getString("condition.params.json")
    //解析JSON字符串，代码行数少
    val jsonObj: JSONObject = JSON.parseObject(jsonString)
    jsonObj.getString(jsonKey)
  }

  /**
    * 从配置文件中根据key获取value
    *
    * @param key
    * @return
    */
  def getValueByKey(key: String): String = {

    /*  val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")

      val properties = new Properties()
      properties.load(stream)
      properties.getProperty(key)
  */
   bundle.getString(key)
  }


}
