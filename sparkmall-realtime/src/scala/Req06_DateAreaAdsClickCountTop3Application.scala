import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object Req06_DateAreaAdsClickCountTop3Application {
  def main(args: Array[String]): Unit = {
    //
    //TODO 每天各地区 top3 热门广告

    //准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("BlackNameListApplication")
    val streamingContext = new StreamingContext(sparkConf, Seconds(5)) //采集周期
    streamingContext.sparkContext.setCheckpointDir("cp")
    // kafka topic
    val topic = "ads_log20190218"

    // TODO 从 kakfa 取数据
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, streamingContext)

    // TODO 处理获取的数据 转换结构
    val adsClickDSteam: DStream[AdsClickKafkaMessage] = kafkaStream.map(data => {
      val datas: Array[String] = data.value().split(" ")
      AdsClickKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    //TODO 1.将数据转换结构(date-area-city-ads,1)
    val dateAdsUserToOneDStream: DStream[(String, Long)] = adsClickDSteam.map(message => {
      val date: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)

    })


   //TODO 2.将转换后的结构后的数据进行聚合（有状态的聚合）
    val stateDStream: DStream[(String, Long)] = dateAdsUserToOneDStream.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.size
        Option(sum)
      }
    }


    //TODO 3. 将聚合后的结果进行结构的转换(date-area-ads,sum)
    val dateAreaAdsToSumDStream: DStream[(String, Long)] = stateDStream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0) + "_" + keys(1) + "_" + keys(3), sum)


      }
    }

    //todo 4.将转换结构后的数据进行聚合(date-area-ads,totalsum)
    val dateAreaAdsToSumTotalSumDStream: DStream[(String, Long)] = dateAreaAdsToSumDStream.reduceByKey(_+_)

    //todo 5.将聚合后的结果进行结构的转换(date-area-ads,totalsum)==》(date-area,(ads,totalsum))
    val dateAreaToAdsTotalsumDStream: DStream[(String, (String, Long))] = dateAreaAdsToSumTotalSumDStream.map {
      case (key, totalSum) => {
        val keys: Array[String] = key.split("_")

        (keys(0) + "_" + keys(1), (keys(2), totalSum))
      }
    }


    //todo 6.将数据进行分组 (date-area,(ads,totalsum))
    val groupDStream: DStream[(String, Iterable[(String, Long)])] = dateAreaToAdsTotalsumDStream.groupByKey()


    //todo 7. 对分组后的数据进行排序（降序）取前3
    val resultDStream: DStream[(String, Map[String, Long])] = groupDStream.mapValues(datas => {
      datas.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3).toMap
    })



    //todo 8.将结果保存到redis中
    resultDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val client: Jedis = RedisUtil.getJedisClient

        datas.foreach{
          case(key,map)=>{
            val keys: Array[String] = key.split("_")

            val k = "top3_ads_per_day"+keys(0)
            val f = keys(1)
           // val v = list   //[(),(),()]


            // todo json=>scala
            import org.json4s.JsonDSL._
            val v: String = JsonMethods.compact(JsonMethods.render(map))
            //         field:   华北      value:  {“2”:1200, “9”:1100, “13”:910}

            client.hset(k,f,v)

          }
        }
   /*     华东
        {"4":302,"1":296,"3":271}
        华北
        {"4":409,"3":384,"1":371}
        华南
        {"3":311,"4":293,"1":283}*/


        client.close()





      })
    })


    // 启动采集器
    streamingContext.start()


    streamingContext.awaitTermination()
  }
}
