package producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object FireAndForgetSending extends App {
  val brokers = "localhost:9092"
  val topic = "fire-and-forget-sending"

  // create producer properties
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

  // create the producer
  val producer = new KafkaProducer[String, String](props)

  // create a producer record
  val record = new ProducerRecord[String, String](topic, "sending messages from fire-and-forget producer")

  // send data - asynchronous
  try {
    producer.send(record)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  // flush data
  producer.flush()

  // flush and close producer
  producer.close()
}
