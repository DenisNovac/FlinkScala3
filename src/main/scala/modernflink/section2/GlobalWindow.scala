package modernflink.section2

import modernflink.model.{AboveAverage, Average, BelowAverage, HumidityReading}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.function.WindowFunction
import org.apache.flinkx.api.serializers.*

// --add-opens=java.base/java.util=ALL-UNNAMED
@main def globalWindowDemo() = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData = inputFile.map(HumidityReading.fromString)

  // compare last coming humidity to overall average per city

  val outputStream =
    humidityData
      .keyBy(_.location)
      .window(GlobalWindows.create())
      .trigger(
        PurgingTrigger.of(CountTrigger.of(5))
      )
      .apply(new GlobalWindowFunction)

  outputStream.print()

  env.execute()
}

class GlobalWindowFunction extends WindowFunction[HumidityReading, String, String, GlobalWindow] {
  override def apply(
      key: String,
      window: GlobalWindow,
      input: Iterable[HumidityReading],
      out: Collector[String]
  ): Unit = {
    // get average by global window
    val avg = input.map(_.humidity).sum / input.size

    val last = input.last

    val result =
      if last.humidity > avg then AboveAverage
      else if last.humidity == avg then Average
      else BelowAverage

    out.collect(s"$key - ${input.map(_.timestamp)} - $avg - $last - $result")
  }
}
