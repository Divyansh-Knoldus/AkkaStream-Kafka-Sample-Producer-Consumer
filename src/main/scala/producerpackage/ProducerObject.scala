package producerpackage

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Producer
import akka.kafka.{ProducerMessage, ProducerSettings}
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import consumerpackage.ConsumerObject

object ProducerObject {
  def main(args: Array[String]): Unit = {
    println("Hello from producer")

    implicit val system: ActorSystem = ActorSystem("producer-sample")

    val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers("localhost:9092")
    val names = List("Girish", "Swantika", "Pinku", "Amit", "Divyansh", "Ashish")

    val done = Source(names)
      .map { n =>
        val k = n.length
        val partition = 0
        ProducerMessage.Message(new ProducerRecord[String, String](
          "topic1", partition, null, k.toString
        ), n)
      }
      .via(Producer.flow(producerSettings))
      .map { result =>
        val record = result.message.record
        println(s"${result.message.passThrough}")
        result
      }
      .runWith(Sink.ignore)
  }
}
