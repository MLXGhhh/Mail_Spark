package SparkToKafka
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
/**
  *@Description SparkStreaming消费kafka数据 测试代码
  *@Author hky
  **/
object KafkaConsumerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Consumer").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val topic = Set("kafka-action")
    val broker = "106.39.31.27:9092"
    val kafkaPrams = Map[String,Object](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
                                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
                                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
                                        ConsumerConfig.GROUP_ID_CONFIG -> "test",
                                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
                                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
                                                                          ssc,
                                                                          PreferConsistent,
                                                                          Subscribe[String, String](topic, kafkaPrams))
    //val value: DStream[String] = stream.map(x => x.value)
    stream.saveAsTextFiles("F:\\kafka-consumer")
    ssc.start()
    ssc.awaitTermination()
  }
}
