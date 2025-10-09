package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.{OutputTag, StreamExecutionEnvironment}

// --add-opens=java.base/java.util=ALL-UNNAMED
@main def sideOutput() = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")

  val humidityDataStream = inputFile.map(HumidityReading.fromString)

  val mainStream = humidityDataStream.process(FireAlert())
  val sideOutputStream = mainStream.getSideOutput(FireAlert.lowHumidity)

  mainStream.print()
  sideOutputStream.print()

  env.execute()

}

// just a pipe that ins and outs HumidityReading, but with side streaming
final case class FireAlert() extends ProcessFunction[HumidityReading, HumidityReading] {
  override def processElement(
      value: HumidityReading,
      ctx: ProcessFunction[HumidityReading, HumidityReading]#Context,
      out: Collector[HumidityReading]
  ): Unit = // those values are only goes to side output, they disappear from main!
    if value.humidity < 50 then ctx.output(FireAlert.lowHumidity, "Fire hazard at " + value.location)
    else out.collect(value)
}

object FireAlert {
  lazy val lowHumidity = new OutputTag[String]("Fire Hazard")
}
