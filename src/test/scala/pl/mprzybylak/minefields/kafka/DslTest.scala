package pl.mprzybylak.minefields.kafka

import java.util.Properties

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{Matchers, WordSpec}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Serde, Serdes, StringSerializer}
import org.apache.kafka.streams.{Consumed, KeyValue, StreamsBuilder, StreamsConfig}
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.streams.kstream._
import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.Materialized._
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.internals.MeteredKeyValueBytesStore
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

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


    "deduplicate with reduce" in {

      val deduplicationWindowDuration = 1 seconds
      val deduplicationRetention = deduplicationWindowDuration.toMillis * 2 + 1
      val deduplicationWindow = TimeWindows.of(deduplicationWindowDuration.toMillis).until(deduplicationRetention)

      val streamsConfiguration = Map[String, String](
        StreamsConfig.COMMIT_INTERVAL_MS_CONFIG -> "1000"
      )

      val evPvDeduplicator: Reducer[String] =
        (first, second) => {
          println("reducer. first: " + first + " second: " + second)
          if(first.equals(second)) first else second
        }

      val evPvToClientKeyMapper: KeyValueMapper[Windowed[String], String, String] =
        (windowedEvPvKey, _) => {
          println("window mapper " + windowedEvPvKey.key )
          windowedEvPvKey.key
        }

      val streamBuilder = new StreamsBuilder()

      val stream: KStream[String, String] =
        streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      val deduplicatedStream: KStream[Windowed[String], String] = stream.groupByKey(Serialized.`with`(stringSerde, stringSerde))
        .windowedBy(deduplicationWindow)
        .reduce(evPvDeduplicator).toStream

      val finalStream: KStream[String, String] = deduplicatedStream.selectKey(evPvToClientKeyMapper)

      finalStream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build(), streamsConfiguration) {

        publishToKafka(inTopic, "1", "duplicate-message1")
        Thread.sleep(100)
        publishToKafka(inTopic, "1", "duplicate-message2")
        Thread.sleep(100)
        publishToKafka(inTopic, "1", "duplicate-message2")

        withConsumer[String, String, Unit] { consumer =>


          val consumedMessages: Stream[(String, String)] =
            consumer.consumeLazily(outTopic)


          consumedMessages.take(3) should be(
            Seq(
              "1" -> "duplicate-message1",
              "1" -> "duplicate-message2"
            )
          )
        }
      }
    }


    "deduplicate with procesor api" in {

      val streamsConfiguration = Map[String, String](
        "cache.max.bytes.buffering" -> "0" // we need to have cache turned off one way or another
      )

      // we need some kind of aggregation - to have state store that we can later work with
      val evPvDeduplicator: Reducer[String] = (first, second) => second

      val streamBuilder = new StreamsBuilder()

      val stream: KStream[String, String] = streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      val afterAggregation: KStream[String, String] = stream.groupByKey(Serialized.`with`(stringSerde, stringSerde))
        .reduce(evPvDeduplicator, "reduce-store")
        .toStream()

      val transformerSupplier: TransformerSupplier[String, String, KeyValue[String, String]] = () => new DuplicateTransformer()
      val deduplicated = afterAggregation.transform(transformerSupplier, "reduce-store")
      deduplicated.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build(), streamsConfiguration) {

        publishToKafka(inTopic, "1", "duplicate-message1")

        publishToKafka(inTopic, "1", "duplicate-message2")
        publishToKafka(inTopic, "1", "duplicate-message2")
        publishToKafka(inTopic, "1", "duplicate-message2")
        publishToKafka(inTopic, "1", "duplicate-message2")

        publishToKafka(inTopic, "1", "duplicate-message1")

        publishToKafka(inTopic, "1", "duplicate-message2")

        withConsumer[String, String, Unit] { consumer =>

          val consumedMessages: Stream[(String, String)] =
            consumer.consumeLazily(outTopic)

          consumedMessages.take(10) should be(
            Seq(
              "1" -> "duplicate-message1",
              "1" -> "duplicate-message2",
              "1" -> "duplicate-message1",
              "1" -> "duplicate-message2"
            )
          )
        }
      }
    }
  }
}

class DuplicateTransformer extends Transformer[String, String, KeyValue[String, String]] {

  var stateStore: MeteredKeyValueBytesStore[String, String] = _

  override def init(context: ProcessorContext): Unit = {
      stateStore = context.getStateStore("reduce-store").asInstanceOf[MeteredKeyValueBytesStore[String, String]]
  }

  override def transform(key: String, value: String): KeyValue[String, String] = {
    val oldValue = stateStore.get(key)

    if(value.equals(oldValue)) {
      return null
    }
    new KeyValue[String, String](key, value)
  }

  override def punctuate(timestamp: Long): KeyValue[String, String] = null


  override def close(): Unit = ()
}
