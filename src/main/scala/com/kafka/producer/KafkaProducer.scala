package com.kafka.producer

import java.util.Properties

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager

object KafkaProducer {

  @transient lazy val log = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      log.error("Usage: KafkaProducer <bootstrap-servers> <protocol> <topic> <messages_per_burst> <sleep_in_ms>")
      log.error("Example: KafkaProducer localhost:9093 SASL_SSL topic1 10 1000")
      System.exit(1)
    }

    val bootstrapServer = args(0)
    val protocol = args(1)
    val topicName = args(2)
    val messagesPerBurst = args(3).toInt
    val sleepInMs = args(4).toInt

    val isUsingSsl = protocol.endsWith("SSL")

    log.info("Creating config properties...")
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol)
    props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    if (isUsingSsl) {
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/etc/cdep-ssl-conf/CA_STANDARD/truststore.jks")
      props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "cloudera")
    }
    log.info("OK")

    log.info("Creating kafka producer...")
    val producer = new KafkaProducer[String, String](props)
    log.info("OK")

    try {
      var startOffset = 0
      while (true) {
        for (i <- 0 until messagesPerBurst) {
          val offset = startOffset + i
          val data = "streamtest-" + offset
          val record = new ProducerRecord[String, String](topicName, null, data)
          producer.send(record, onSendCallback(offset))
          log.info("Record: " + record)
        }
        Thread.sleep(sleepInMs)
        startOffset += messagesPerBurst
      }
    } finally {
      log.info("Closing kafka producer...")
      producer.close()
      log.info("OK")
    }
  }

  def onSendCallback(messageNumber: Int): Callback = new Callback() {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        log.debug(s"Message $messageNumber sent to topic ${metadata.topic()} in partition ${metadata.partition()} at offset ${metadata.offset()}")
      } else {
        log.error(s"Error sending message $messageNumber")
        exception.printStackTrace()
      }
    }
  }
}
