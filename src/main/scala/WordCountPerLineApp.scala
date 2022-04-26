

object WordCountPerLineApp extends App {

  import org.apache.kafka.clients.consumer.ConsumerConfig
  import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, scala}
  import java.util.Properties
  import _root_.scala.concurrent.duration.DurationInt

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordCountPerLineAPPP")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5.seconds)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  val builder = new scala.StreamsBuilder()
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