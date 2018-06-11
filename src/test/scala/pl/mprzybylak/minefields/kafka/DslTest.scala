package pl.mprzybylak.minefields.kafka

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{Matchers, WordSpec}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Serde, Serdes, StringSerializer}
import org.apache.kafka.streams.{Consumed, StreamsBuilder}
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{KStream, Produced}
import org.scalatest.{Matchers, WordSpec}
import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder

import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{Consumed, StreamsBuilder}
import org.apache.kafka.streams.kstream.{KStream, Produced}
import org.scalatest.{Matchers, WordSpec}

class DslTest extends WordSpec with EmbeddedKafkaStreamsAllInOne with Matchers {

  implicit val serializer: StringSerializer = new StringSerializer

  "dsl api" should {

    val (inTopic, outTopic) = ("in", "out")
    val stringSerde: Serde[String] = Serdes.String()

      "allow for duplicate in default topology" in {
        val streamBuilder = new StreamsBuilder
        val stream: KStream[String, String] =
          streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

        stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

        runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {

          publishToKafka(inTopic, "1", "duplicate-message")
          publishToKafka(inTopic, "1", "duplicate-message")
          publishToKafka(inTopic, "1", "duplicate-message")

          withConsumer[String, String, Unit] { consumer =>
            val consumedMessages: Stream[(String, String)] =
              consumer.consumeLazily(outTopic)


            consumedMessages.take(3) should be(
              Seq(
                "1" -> "duplicate-message",
                "1" -> "duplicate-message",
                "1" -> "duplicate-message"
              )
            )
          }
        }
      }
  }
}
