package modernflink.section2

import modernflink.model.HumidityReading
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

// --add-opens=java.base/java.util=ALL-UNNAMED
@main def listState() = {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val data: DataStream[HumidityReading] = inputFile.map(HumidityReading.fromString)

  val humidityChange =
    data
      .keyBy[String](_.location)
      .process(new KeyedProcessFunction[String, HumidityReading, String] {

        var humidityOutputStream: ListState[HumidityReading] = _

        override def open(parameters: Configuration): Unit = {
          humidityOutputStream = getRuntimeContext.getListState(
            new ListStateDescriptor[HumidityReading](
              "humidityChangeOutputStream",
              classOf[HumidityReading]
            )
          )
        }

        override def processElement(
            value: HumidityReading,
            ctx: KeyedProcessFunction[String, HumidityReading, String]#Context,
            out: Collector[String]
        ): Unit = {
          humidityOutputStream.add(value)

          val humidityRecords =
            humidityOutputStream.get().asScala.toList // java Iterable

          if humidityRecords.size > 10 then humidityOutputStream.clear()

          out.collect(s"${value.location} - ${humidityRecords.size} - ${humidityRecords.mkString}")
        }

      })

  humidityChange.print()

  env.execute()

}
