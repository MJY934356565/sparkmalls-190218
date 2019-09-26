import java.util

import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object Req04_RedisBlackNameListApplication {
  def main(args: Array[String]): Unit = {
    // 需求四广告黑名单实时统计
    //TODO 实现实时的动态黑名单机制：将每天对某个广告点击超过 100 次的用户拉黑

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


    //TODO 0.对数据进行筛选过滤，黑名单数据不需要
    //TODO 问题  这段代码在Driver执行只执行一遍，后面的更新不知道，黑名单在不断更新，需要周期性获取黑名单数据
    /*    val jedisClient: Jedis = RedisUtil.getJedisClient

        val userids: util.Set[String] = jedisClient.smembers("blacklist")
       // userids.contains("1")


        //TODO 使用广播变量,可以解决这个问题
        val useridsBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(userids)*/
    /*     //TODO  会发生空指针异常的原因是因为序列化规则
       val filterDstream: DStream[AdsClickKafkaMessage] = adsClickDSteam.filter(message => {
       // println(message)
         //TODO 这段代码在executor执行
         !useridsBroadcast.value.contains(message.userid)
       })*/
    //TODO Driver一次
    val filterDstream: DStream[AdsClickKafkaMessage] = adsClickDSteam.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val userids: util.Set[String] = jedisClient.smembers("blacklist")
      // userids.contains("1")

      jedisClient.close()
      //TODO 使用广播变量,可以解决这个问题
      val useridsBroadcast: Broadcast[util.Set[String]] = streamingContext.sparkContext.broadcast(userids)

      //TODO Driver  N次
      rdd.filter(message => {
        //TODO Executor N次
        !useridsBroadcast.value.contains(message.userid)
      })
    })
    //TODO 1.将数据转换结构(date-ads-user,1)
    val dateAdsUserToOneDStream: DStream[(String, Long)] = filterDstream.map(message => {
      val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.adid + "_" + message.userid, 1L)

    })



    //TODO 2.将转换结构后的数据进行redis聚合(date-ads-user,sum)
    dateAdsUserToOneDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val client: Jedis = RedisUtil.getJedisClient
        val key = "date:ads:user:click"

        datas.foreach {
          case (field, one) => {
            client.hincrBy(key, field, 1L)
            //TODO 3.对聚合后的结果进行阈值的判断，如果查出阈值，将用户拉入黑名单

            val sum: Long = client.hget(key, field).toLong

            //TODO 如果查出阈值，将用户拉入黑名单

            if (sum >= 100) {
              val keys: Array[String] = field.split("_")
              val userid = keys(2)

              client.sadd("blacklist", userid)

            }

          }
        }

        client.close()

      })
      /* rdd.foreach{
         case(field,one)=>{
           //将数据在redis中聚合
           val client: Jedis = RedisUtil.getJedisClient
           val key = "date:ads:user:click"

           client.hincrBy(key,field,1L)

           //3.对聚合后的结果进行阈值的判断，如果查出阈值，将用户拉入黑名单

           val sum: Long = client.hget(key,field).toLong

           //如果查出阈值，将用户拉入黑名单

           if(sum>=100){
             val keys: Array[String] = field.split("_")
             val userid = keys(2)

             client.sadd("blacklist",userid)

           }


             client.close()
         }
       }*/
    })



    // 启动采集器
    streamingContext.start()


    streamingContext.awaitTermination()
  }
}
