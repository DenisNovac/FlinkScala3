package modernflink.section1

import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment}
import modernflink.model.{HumidityReading, LocalSummary, TemperatureReading}

// --add-opens=java.base/java.util=ALL-UNNAMED
@main def multipleStreams = {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val humidityStream: KeyedStream[HumidityReading, String] =
    env
      .readTextFile("src/main/resources/Humidity.txt")
      .map(HumidityReading.fromString)
      .keyBy(_.location)

  val humidityStream2: KeyedStream[HumidityReading, String] =
    env
      .readTextFile("src/main/resources/MoreHumidityData.txt")
      .map(HumidityReading.fromString)
      .keyBy(_.location)

  val tempStream: KeyedStream[TemperatureReading, String] =
    env
      .readTextFile("src/main/resources/Temperature.txt")
      .map(TemperatureReading.fromString)
      .keyBy(_.location)

  // just combines two streams into one,
  // doesn't somehow combine elements between themselves or anything
  def unionExample() = {

    val unionHumidity =
      humidityStream.union(humidityStream2)

    unionHumidity.print()

    env.execute()
  }

  // again combines two streams into one with same data type
  // but not elements between themselves
  def connectExample() = {

    val humidityAndTemp: ConnectedStreams[HumidityReading, TemperatureReading] =
      humidityStream
        .union(humidityStream2)
        .connect(tempStream)

    val outputConnectedStream: DataStream[LocalSummary] =
      humidityAndTemp
        .map[LocalSummary](_.toLocalSummary, _.toLocalSummary)
        .keyBy(_.location)

    outputConnectedStream.print()
    env.execute()
  }

  connectExample()
  // unionExample()

}
