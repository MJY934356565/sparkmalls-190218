import java.util

import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import redis.clients.jedis.Jedis

object Req05_AdsClickCountApplication {
  def main(args: Array[String]): Unit = {
    //
    //TODO 每天各地区各城市各广告的点击流量实时统计。

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackNameListApplication")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5)) //采集周期
    streamingContext.sparkContext.setCheckpointDir("cp")
    // kafka topic
    val topic = "ads_log20190218"

    //TODO 从 kakfa 取数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    //TODO 处理获取的数据 转换结构
    val adsClickDSteam: DStream[AdsClickKafkaMessage] = kafkaStream.map(data => {
      val datas: Array[String] = data.value().split(" ")
      AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    //TODO 1.将数据转换结构(date-area-city-ads,1)
    val dateAdsUserToOneDStream: DStream[(String, Long)] = adsClickDSteam.map(message => {
      val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)

    })


    //TODO 2.将转换结构后的数据进行有状态聚合(date-area-city-ads,sum)
    val reduceDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.reduceByKey(_+_)

    //TODO 3.更新redis中的最终统计结果
    reduceDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {
        val client: Jedis = RedisUtil.getJedisClient
        datas.foreach {
          case (field, sum) => {
            client.hincrBy("date:area:city:ads", field, sum)
          }
        }
        client.close()

      })
    })

    //TODO 启动采集器
    streamingContext.start()


    streamingContext.awaitTermination()
  }
}
