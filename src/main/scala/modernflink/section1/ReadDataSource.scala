package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.serializers.*

import java.time.Instant
// flink-scala-api imports
import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*

import modernflink.model.SubscriptionEvent
import modernflink.model.SubscriptionEventsGenerator

// --add-opens=java.base/java.util=ALL-UNNAMED
object ReadDataSource1 {

  // for some reason .map creates endless waiting
  // unless put into @main method instead of just object
  @main def main() = {
    // read humidity file
    // location, time, humidity
    // Flagstaff, 1686208915, 59

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // read from file
    val testStream2 =
      env
        .readTextFile("src/main/resources/Humidity.txt")
        .map[HumidityReading](s => HumidityReading.fromString(s))

    testStream2
      .print()

//    testStream2
//      .setParallelism(12)
//      .executeAndCollect()
//      .toVector
//      .foreach(println)

    env.execute()

  }

}

// --add-opens=java.base/java.util=ALL-UNNAMED
object ReadDataSource2 {

  @main def main2() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // flink provides some common types
    given instantTypeInfo: TypeInformation[Instant] = BasicTypeInfo.INSTANT_TYPE_INFO

    // SourceFunction allows to generate some stub data and put it directly into DataStream
    // SubscriptionsEventGenerator is a generator of random events
    // example of output:
    //  10> CancelEvent(Joan,2025-10-08T13:48:47.215307Z,d76f410e-1855-4316-8eda-6c3f21e6d2f0)
    //  1> PaymentEvent(Peggy,2025-10-08T13:48:48.215307Z,ca5ae489-a534-45ed-b75e-798a1844abea)
    val genEventsStream = SubscriptionEventsGenerator(sleepSeconds = 1, startTime = Instant.now())

    val testStreamFour: DataStream[SubscriptionEvent] =
      env.addSource(genEventsStream)

    testStreamFour
      .print()

    env.execute()
  }

}
