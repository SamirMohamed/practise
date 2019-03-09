package producer

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

object AsynchronousSending extends App with LazyLogging {
  val brokers = "localhost:9092"
  val topic = "synchronous-sending"

  // create producer properties
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

  // create the producer
  val producer = new KafkaProducer[String, String](props)

  // create a producer record
  val record = new ProducerRecord[String, String](topic, "sending messages from asynchronous producer")

  // send data
  try {
    producer.send(record, (recordMetadata: RecordMetadata, e: Exception) => {
      // executes every time a record is successfully sent or an exception is thrown
      if (e == null) {
        // the record was successfully sent
        logger.info("Received new metadata. \n" +
          "Topic:" + recordMetadata.topic() + "\n" +
          "Partition: " + recordMetadata.partition() + "\n" +
          "Offset: " + recordMetadata.offset() + "\n" +
          "Timestamp: " + recordMetadata.timestamp())
      } else {
        logger.error("Error while producing", e)
      }
    })
  } catch {
    case e: Exception => e.printStackTrace()
  }


  // flush data
  producer.flush()

  // flush and close producer
  producer.close()

}
