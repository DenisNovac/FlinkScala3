package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.serializers.*
// flink-scala-api imports
import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

object ReadDataSource extends App {
  // read humidity file
  // location, time, humidity
  // Flagstaff, 1686208915, 59

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // read from file
  val testStreamOne: DataStream[HumidityReading] = env.fromCollection(
    Seq(
      HumidityReading("Flagstaff", 1686208915, 59),
      HumidityReading("Flagstaff", 1686295315, 53)
    )
  )

  testStreamOne
    // endless wait?
    // .map[HumidityReading](x => x.copy(humidity = x.humidity + 1))
    .print("OutputStream1")
    .setParallelism(2)

  env.execute()

}
