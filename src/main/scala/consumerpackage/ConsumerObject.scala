package consumerpackage

import akka.actor.ActorSystem
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, Materializer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import producerpackage.ProducerObject

object ConsumerObject extends App {
  override def main(args: Array[String]): Unit = {
    println("Hello from Consumer")

    implicit val system: ActorSystem = ActorSystem("consumer-sample")
    implicit val materializer: Materializer = ActorMaterializer()

    val producerSettings =
      ProducerSettings(system, new StringSerializer, new StringSerializer)
        .withBootstrapServers("localhost:9092")

    val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


    Consumer.committableSource(consumerSettings, Subscriptions.topics("topic1"))
      .map { msg =>
        println(s" no of character in that words are : ${msg.record.value()}")
        ProducerMessage.Message(new ProducerRecord[String, String](
          "topic2",
          msg.record.value
        ), msg.committableOffset)
      }
      .runWith(Producer.committableSink(producerSettings))
  }
}
