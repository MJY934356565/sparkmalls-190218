import com.atguigu.bigdata.sparkmall.common.util.{DateUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object Req07AdsClickChartApplication {
  def main(args: Array[String]): Unit = {
    //
    //TODO 最近1分钟广告点击的趋势（每10秒）

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


    //todo 1.使用窗口函数将数据进行封装
                                                                              // 1分钟，滑动步长10秒
    val windowDStream: DStream[AdsClickKafkaMessage] = adsClickDSteam.window(Seconds(60),Seconds(10))

    //todo 2. 将数据进行结构的转换（15:11==》15:10） （15.25==》15.20）
    val timeToOneDStream: DStream[(String, Long)] = windowDStream.map(message => {
                                        //formatStringByTimestamp给时间戳返回字符串
      val timeString: String = DateUtil.formatStringByTimestamp(message.timestamp.toLong)
      val time: String = timeString.substring(0, timeString.length - 1)+"0"

      (time, 1L)

    })
    //todo 3.将转换结构后的数据进行聚合统计                            //reducebykey会打乱重组
    val timeToSumDStream: DStream[(String, Long)] = timeToOneDStream.reduceByKey(_+_)

    //todo 4.对统计结果进行排序  transform 有返回结果，遍历一个批次只能是DStream时用这个方法
    val sortDStream: DStream[(String, Long)] = timeToSumDStream.transform(rdd => {
      rdd.sortByKey()
    })

    sortDStream.print()

    // 启动采集器
    streamingContext.start()

    //Driver应该等待采集器的执行结束
      streamingContext.awaitTermination()
  }
}
