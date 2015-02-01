import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._


object graphUpdates {
  def main(args : Array[String]) {
        val conf = new SparkConf().setMaster("local[2]").setAppName("stream_example")
        val ssc = new StreamingContext(conf, Seconds(5))

        val zkQuorum = "localhost:2181"
        val groupID = "graph"
        val topics = Map("updates" -> 1)
        val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupID, topics)

        kafkaStream.print()
        ssc.start()
        ssc.awaitTermination()
  }
}


