package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, KafkaSinkBuilder}
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

// --add-opens=java.base/java.util=ALL-UNNAMED
@main def writeCustomDataToKafka() = {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val inputFile: DataStream[String] = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData: DataStream[HumidityReading] = inputFile.map(HumidityReading.fromString)

  val sink =
    KafkaSink
      .builder[String]()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema
          .builder()
          .setTopic("humidity-reading")
          .setValueSerializationSchema(SimpleStringSchema())
          .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.NONE)
      .build()

  humidityData
    .map(_.sinkOutput)
    .sinkTo(sink)

  env.execute()
}
