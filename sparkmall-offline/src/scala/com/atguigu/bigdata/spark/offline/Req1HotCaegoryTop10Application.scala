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

object Req1HotCaegoryTop10Application {
  def main(args: Array[String]): Unit = {
    //todo 需求1，获取点击，下单和支付数量排名前10的品类


    //todo 4.0创建SparkSql环境对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1HotCaegoryTop10Application")
    //将外部的hive.site文件传进来，启用hive的支持
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //RDD ,DFrame ,Dset 转换，隐式转换
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

    println(actionDF.count())


    //todo 4.2使用累加器累加数据，进行数据的聚合(categoryid-click,100), (categoryid-order,100), (categoryid-pay,100)
    val accumulator = new CategoryAccumulator
    spark.sparkContext.register(accumulator, "Category")
    actionRDD.foreach {
      actionData => {
        if (actionData.click_category_id != -1) {
          accumulator.add(actionData.click_category_id + "-click")
        } else if (StringUtil.isNotEmpty(actionData.order_category_ids)) {
          val ids: Array[String] = actionData.order_category_ids.split(",")
          for (id <- ids) {
            accumulator.add(id + "-order")
          }

        } else if (StringUtil.isNotEmpty(actionData.pay_category_ids)) {
          val ids: Array[String] = actionData.pay_category_ids.split(",")
          for (id <- ids) {
            accumulator.add(id + "-pay")
          }

        }


      }
    }
    //println(accumulator.value)
    //todo (catagory-指标，sumcount)
    val accuData: mutable.HashMap[String, Long] = accumulator.value
    //todo 4.3将累加器的结果通过ID进行分组(categoryid,[(order,100),(click:100),(pay:100)])
    val categoryToAccuData: Map[String, mutable.HashMap[String, Long]] = accuData.groupBy {
      case (key, sumcount) => {
        val keys: Array[String] = key.split("-")
        keys(0)
      }
    }

    //todo 4.4将分组后的结果转化为对象CategoryTop10(categoryid,click,pay)
    val taskId: String = UUID.randomUUID().toString

    val categoryTOp10Datas: immutable.Iterable[CategoryTop10] = categoryToAccuData.map {
      case (categoryId, map) => {
        CategoryTop10(taskId,
          categoryId,
          map.getOrElse(categoryId + "-click", 0L),
          map.getOrElse(categoryId + "-order", 0L),
          map.getOrElse(categoryId + "-pay", 0L))
      }
    }


    //todo 4.5将转换后的对象进行排序(点击，下单，支付)（降序）
    val sortList: List[CategoryTop10] = categoryTOp10Datas.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }

      }
    }
    //todo 4.6将排序后的结果取前10保存到Mysql中
    val top10List: List[CategoryTop10] = sortList.take(10)
    println(top10List)

    val driverClass = ConfigUtil.getValueByKey("jdbc.driver.class")
    val url = ConfigUtil.getValueByKey("jdbc.url")
    val user = ConfigUtil.getValueByKey("jdbc.user")
    val password = ConfigUtil.getValueByKey("jdbc.password")

    Class.forName(driverClass)
    val connection: Connection = DriverManager.getConnection(url, user, password)


    val insertSQL = "insert into category_top10 (taskId,category_id,click_count,order_count,pay_count) values (?,?,?,?,?)"

    val pstat: PreparedStatement = connection.prepareStatement(insertSQL)

    top10List.foreach {
      data => {
        pstat.setString(1, data.taskId)
        pstat.setString(2, data.categoryId)
        pstat.setLong(3, data.clickCount)
        pstat.setLong(4, data.orderCount)
        pstat.setLong(5, data.payCount)
        pstat.executeUpdate()

      }
    }

    pstat.close()
    connection.close()
    //4.7释放资源
    spark.stop()


  }
}

case class CategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

//todo 声明累加器，用于聚合相同品类的不同指标数据
//todo (categoryid-click,100), (categoryid-order,100), (categoryid-pay,100)
class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var map = new mutable.HashMap[String, Long]()
  //在Java序列化的时候会触发 ---在闭包检测的时候会触发，copy-》reset-》isZero
  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }
      //输入
  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1

  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //两个map的合并
    val map1 = map
    val map2 = other.value
    map = map1.foldLeft(map2) {
      (innerMap, t) => {
        innerMap(t._1) = innerMap.getOrElse(t._1, 0L) + t._2
        innerMap
      }
    }

  }
            //输出
  override def value: mutable.HashMap[String, Long] = {
    map
  }
}