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
  import org.apache.kafka.streams.kstream.Materialized
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala._
  import org.apache.kafka.streams.scala.kstream._
  import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordCountPerLineAPP")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  val builder = new StreamsBuilder
  val textLines: KStream[Array[Byte], String] = builder.stream[Array[Byte], String]("inputTopic")

  val wordCounts: KTable[String, Long] = textLines
    .flatMapValues(value => value.toLowerCase.split("\\W+"))
    .groupBy((_, word) => word)
    .count()
  wordCounts.toStream.to("outputTopic")

  val topology = builder.build
  val streams = new KafkaStreams(topology, props)

  val latch = new CountDownLatch(1)
  Runtime.getRuntime.addShutdownHook(new Thread("streams-shutdown-hook") {
    override def run(): Unit = {
      streams.close()
      latch.countDown()
    }
  })
  try {
    streams.start()
    latch.await()
  } catch {
    case e: Throwable =>
      System.exit(1)
  }
  System.exit(0)
}
