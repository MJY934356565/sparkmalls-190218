package com.atguigu.bigdata.spark.offline

import com.atguigu.bigdata.sparkmall.common.model.UserVisitAction
import com.atguigu.bigdata.sparkmall.common.util.{ConfigUtil, DateUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Req8PageFAvgTimeApplication {
  def main(args: Array[String]): Unit = {



    //4.0创建SparkSql环境对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1HotCaegoryTop10Application")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import spark.implicits._


    //4.1从hive中获取满足条件的数据
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


    //todo 页面平均停留时间

    //todo 1.将数据根据session进行分组
    // 同一个用户的浏览记录，如果是不同用户页面跳转会互串出错
    //(k,v)===>(sessionid,每个用户下的每个页面时间信息)
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

    //todo 2.将分组后的数据进行时间排序（升序）
                                        //(k,v)==>(sessionid,((1,20),(2,30),(3,40)))
    val sessionToPageidAndTimeXRDD: RDD[(String, List[(Long, Long)])] = groupRDD.mapValues(datas => {
      val sortList: List[UserVisitAction] = datas.toList.sortWith {
        (left, right) => {
          left.action_time < right.action_time
        }
      }

      //todo 3.将页面数据进行拉链(（1-2），（time2-time1）)
      val idToTimeList: List[(Long, String)] = sortList.map(action => {
        (action.page_id, action.action_time)
      })

      //((pageid1,time1),(pageid2,time2))
      val pageid1ToPageid2List: List[((Long, String), (Long, String))] = idToTimeList.zip(idToTimeList.tail)


      //todo 4.将拉链数据进行结构的转变（1，(timeX)）,（1，(timeX)）,（1，(timeX)）
      pageid1ToPageid2List.map {
        case (page1, page2) => {
          val time1: Long = DateUtil.parseLongByString(page1._2)
          val time2: Long = DateUtil.parseLongByString(page2._2)

          val timeX: Long = time2 - time1

          (page1._1, timeX)
        }

      }
    })


    //todo 5.将转变结构后的数据进行分组（pageid,Iterator[(time)]）
    val pageidToTimeXListRDD: RDD[List[(Long, Long)]] = sessionToPageidAndTimeXRDD.map {
      case (k, v) => v
    }
   // pageidToTimeXListRDD.collect().foreach(println)
    val pageidToTimeXRDD: RDD[(Long, Long)] = pageidToTimeXListRDD.flatMap(list=>list)

    val groupPageRDD: RDD[(Long, Iterable[Long])] = pageidToTimeXRDD.groupByKey()


    //todo 6.获取最终结果：（pageid,timeSum/timeSize）  集合中的时间总和除以次数
    groupPageRDD.foreach{
      case(pageid,timexs)=>{
        println("页面"+pageid+"平均停留时间="+(timexs.sum/timexs.size))
      }
    }

    //4.7释放资源
    spark.stop()


  }
}

