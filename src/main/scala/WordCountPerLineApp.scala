import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object WordCountPerLineApp extends App {

  import org.apache.kafka.streams.scala._
  import ImplicitConversions._
  import serialization.Serdes._

  val builder = new StreamsBuilder()
  builder
    .stream[String, String]("hello")
    .flatMapValues {_.split("\\W+")}
    .groupBy { case (_, value) => value }
    .count
    .mapValues(_.toString)
    .toStream
    .to("bye")

  val topology = builder.build

  import java.util.Properties
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordCountPerLineAPPP")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  import scala.concurrent.duration._
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5.seconds.toMillis)
  import org.apache.kafka.clients.consumer.ConsumerConfig
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
  val streams = new KafkaStreams(topology, props)
  streams.start()
}