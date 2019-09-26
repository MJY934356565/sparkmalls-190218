import java.util

import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object Req04_BlackNameListApplication {
  def main(args: Array[String]): Unit = {
    // 需求四广告黑名单实时统计
    // 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑

    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackNameListApplication")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5)) //采集周期
    streamingContext.sparkContext.setCheckpointDir("cp")
    // kafka topic
    val topic = "ads_log20190218"

    // 从 kakfa 取数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    // 处理获取的数据 转换结构
    val adsClickDSteam: DStream[AdsClickKafkaMessage] = kafkaStream.map(data => {
      val datas: Array[String] = data.value().split(" ")
      AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

  //  adsClickDSteam.foreachRDD(rdd=>rdd.foreach(println))


    //0.对数据进行筛选过滤，黑名单数据不需要
      //问题  这段代码在Driver执行只执行一遍，后面的更新不知道，黑名单在不断更新，需要周期性获取黑名单数据
/*    val jedisClient: Jedis = RedisUtil.getJedisClient

    val userids: util.Set[String] = jedisClient.smembers("blacklist")
   // userids.contains("1")


    //使用广播变量,可以解决这个问题
    val useridsBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(userids)*/
 /*     //会发生空指针异常的原因是因为序列化规则
    val filterDstream: DStream[AdsClickKafkaMessage] = adsClickDSteam.filter(message => {
    // println(message)
      //这段代码在executor执行
      !useridsBroadcast.value.contains(message.userid)
    })*/
    //Driver一次
    val filterDstream: DStream[AdsClickKafkaMessage] = adsClickDSteam.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val userids: util.Set[String] = jedisClient.smembers("blacklist")
      // userids.contains("1")

      jedisClient.close()
      //使用广播变量,可以解决这个问题
      val useridsBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(userids)

      //Driver  N次
      rdd.filter(message => {
        //Executor N次
        !useridsBroadcast.value.contains(message.userid)
      })
    })
    //1.将数据转换结构(date-ads-user,1)
    val dateAdsUserToOneDStream: DStream[(String, Long)] = filterDstream.map(message => {
      val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.adid + "_" + message.userid, 1L)

    })



    //2.将转换结构后的数据进行有状态聚合(date-ads-user,sum)
    val stateDstream: DStream[(String, Long)] = dateAdsUserToOneDStream.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.size
        Option(sum)
      }
    }

    //3.对聚合后的结果进行阈值的判断，如果查出阈值，将用户拉入黑名单

    //redis 五大数据类型  set
    //jedis  java客户端工具
    stateDstream.print()
    stateDstream.foreachRDD(rdd=>{
      rdd.foreach{
        case(key,sum)=>{
          if(sum>=100){
            //如果查出阈值，将用户拉入黑名单
            val keys: Array[String] = key.split("_")
            val userid = keys(2)

            val client: Jedis = RedisUtil.getJedisClient
            client.sadd("blacklist",userid)
            client.close()
          }
        }
      }
    })


    // 启动采集器
    streamingContext.start()










    streamingContext.awaitTermination()
  }
}
case class AdsClickKafkaMessage(timestamp: String, area: String, city: String, userid: String, adid: String)
