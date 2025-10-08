package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.serializers.*
// flink-scala-api imports
import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

// only runs on JDK 11
object ReadDataSource extends App {
  // read humitidy file
  // location, time, humidity
  // Flagstaff, 1686208915, 59

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // read from file
  val testStreamOne = env.fromCollection(
    Seq(
      HumidityReading("Flagstaff", 1686208915, 59),
      HumidityReading("Flagstaff", 1686295315, 53)
    )
  )

  testStreamOne.print("OutputStream1").setParallelism(2)

  env.execute()

}
