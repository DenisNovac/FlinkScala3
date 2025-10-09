package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{DeserializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}

// --add-opens=java.base/java.util=ALL-UNNAMED
@main def myKafkaSource() = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val source =
    KafkaSource
      .builder[HumidityReading]()
      .setBootstrapServers("localhost:9092")
      .setTopics("humidity-reading")
      .setGroupId("humidity-group")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new CustomDeserializer)
      .setProperty("receive.message.max.bytes", "200M")
      .build()

  val stream: DataStream[HumidityReading] =
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka")

  stream.print()

  env.execute()

}

class CustomDeserializer extends DeserializationSchema[HumidityReading] {

  override def deserialize(message: Array[Byte]): HumidityReading =
    HumidityReading.fromString(new String(message))

  override def isEndOfStream(nextElement: HumidityReading): Boolean =
    nextElement == HumidityReading.error

  override def getProducedType: Typeclass[HumidityReading] =
    implicitly[TypeInformation[HumidityReading]]
}
