package com.atguigu.bigdata.spark.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Req3PageFlowApplication {
  def main(args: Array[String]): Unit = {
    //todo 页面单跳转化率


    //todo 4.0创建SparkSql环境对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1HotCaegoryTop10Application")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    spark.sparkContext.setCheckpointDir("cp")
    import spark.implicits._


    //todo 4.1从hive中获取满足条件的数据
    spark.sql("use " + ConfigUtil.getValueByKey("hive.database"))
    var sql = "select * from user_visit_action where 1 = 1"

    //获取条件


    val startDate: String = ConfigUtil.getValueByJsonKey("startDate")
    val endDate: String = ConfigUtil.getValueByJsonKey("endDate")

    if (StringUtil.isNotEmpty(startDate)) {
      sql = sql + " and date >= '" + startDate + "' "
    }
    if (StringUtil.isNotEmpty(endDate)) {
      sql = sql + " and date <= '" + endDate + "' "
    }
    val actionDF: DataFrame = spark.sql(sql)

    val actionDS: Dataset[UserVisitAction] = actionDF.as[UserVisitAction]
    val actionRDD: RDD[UserVisitAction] = actionDS.rdd

    // println(actionDF.count())


    //*********************************需求三代码****************************************************
    //todo    4.1计算分母的数据
    //todo      4.1.1 将原始数据进行结构的转换，用于统计分析（pageid,1）

    val pageids: Array[String] = ConfigUtil.getValueByJsonKey("targetPageFlow").split(",")


    //todo (1-2),(2-3)
    val pageflowIds: Array[String] = pageids.zip(pageids.tail).map {
      case (pageid1, pageid2) => {
        pageid1 + "-" + pageid2
      }
    }
    //pageflowIds.foreach(println)


    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
      pageids.contains(action.page_id.toString)
    })
    //filterRDD.foreach(println)
    val pageMapRDD: RDD[(Long, Long)] = filterRDD.map(action => {
      (action.page_id, 1L)
    })

    //   pageMapRDD.foreach(println)


    //todo    4.1.2将转换后的结果进行聚合统计(pageid,1)-（pageid,sumcount）

    val pageIdToSumRDD: RDD[(Long, Long)] = pageMapRDD.reduceByKey(_ + _)
    val pageIdToSumMap: Map[Long, Long] = pageIdToSumRDD.collect().toMap
    // pageIdToSumRDD.foreach(println)
    // println("-------------------------------")
    //pageIdToSumMap.foreach(println)
    //todo    4.2计算分子
    //todo      4.2.1将原始数据通过session进行分组（sessionId,iterator[pageid,action_time]）
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(
      action =>
        action.session_id
    )
    //groupRDD.foreach(println)


    //todo    4.2.2将分组后的数据使用时间进行排序（升序）

    val sessionToPageFlowRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues {
      datas => {
        val actions: List[UserVisitAction] = datas.toList.sortWith {
          (left, right) => {
            left.action_time <= right.action_time
          }
        }
        //  1,2,3,4,5,6,7
        // 2,3,4,5,6,7
        val sessionPageids: List[Long] = actions.map(_.page_id)
        //todo (1-2,1),(2-3,1),3-4,4-5
        //todo    4.2.3将排序后的数据进行拉链处理，形成单跳页面流转顺序
        val pageid1ToPageid2s: List[(Long, Long)] = sessionPageids.zip(sessionPageids.tail)

        pageid1ToPageid2s.map {
          case (pageid1, pageid2) => {
            //todo    4.2.4将处理后的数据进行筛选过滤，保留需要关系的流转数据（session,pageid1-pageid2,1）
            (pageid1 + "-" + pageid2, 1L)
          }
        }

      }
    }

    //todo    4.2.5对过滤后的数据进行结构的转化（pageid1-pageid2,1）
    val mapRDD: RDD[List[(String, Long)]] = sessionToPageFlowRDD.map(_._2)

    //todo (1-2,1),(1-9,1),(2-8,1)
    val flatMapRDD: RDD[(String, Long)] = mapRDD.flatMap(list => list)
    val finalPageflowRDD: RDD[(String, Long)] = flatMapRDD.filter {
      case (pageflow, one) => {
        pageflowIds.contains(pageflow)
      }
    }

    //todo    4.2.6将转换结构后的数据进行聚合统计（pageid1-pageid2,sumcount1）
    val resultRDD: RDD[(String, Long)] = finalPageflowRDD.reduceByKey(_ + _)


    //todo    4.3使用分子数据除以分母数据：（sumcount1/sumcount）
    resultRDD.foreach {
      case (pageflow, sum) => {
        val ids: Array[String] = pageflow.split("-")
        val pageid: String = ids(0)
        val sum1: Long = pageIdToSumMap.getOrElse(pageid.toLong, 1L)

        //1-2
        //println(pageflow+"="+(sum.toDouble/sum1))


      }
    }


    //*************************************************************************************

    //4.7释放资源
    spark.stop()


  }
}

