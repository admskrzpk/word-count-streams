import org.apache.kafka.clients.consumer.ConsumerConfig

object WordCountPerLineApp extends App {

  import org.apache.kafka.common.serialization.Serdes
  import org.apache.kafka.streams.kstream.{Materialized, Produced}
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.state.KeyValueStore
  import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
  import java.util
  import java.util.{Locale, Properties}
  import java.util.concurrent.CountDownLatch
  import org.apache.kafka.streams.scala._
  import org.apache.kafka.streams.scala.kstream._
  import org.apache.kafka.streams.scala._
  import ImplicitConversions._
  import serialization.Serdes._
  import org.apache.kafka.streams.scala.kstream._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.StreamsBuilder
  import org.apache.kafka.streams.scala.kstream._

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordCountPerLineAPPP")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  val builder = new StreamsBuilder
  builder
    .stream[String, String]("hello")
    .flatMapValues {_.split("\\W+")}
    .groupBy { case (_, value) => value }
    .count
    //opcjonalnie
    .mapValues(_.toString)
    .toStream
    .to("bye")

  val topology = builder.build
  val streams = new KafkaStreams(topology, props)
  streams.start()
}