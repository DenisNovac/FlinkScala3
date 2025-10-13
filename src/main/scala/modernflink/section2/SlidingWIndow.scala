package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.function.AllWindowFunction
import org.apache.flinkx.api.serializers.*

import java.time.Instant

@main def slidingWindowDemo() = {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // we want result for the previous five seconds

  // sliding window gives overlapping windows

  val depositData = env
    .addSource(
      new DepositEventGenerator(
        sleepSeconds = 1,
        startTime = Instant.parse("2023-08-13T00:00:00.00Z")
      )
    )
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Deposit] {
          override def extractTimestamp(
              element: Deposit,
              recordTimestamp: Long
          ) =
            element.time.toEpochMilli
        })
    )

  val stream =
    depositData
      .windowAll(
        SlidingEventTimeWindows.of(
          Time.milliseconds(2000), // size
          Time.milliseconds(1000) // slide
        )
      )

  val f = stream.apply(new DepositBySlidingWindow)

  f.print()
  env.execute()

  // size is bigger than slide so we are showing results of last 2 seconds each 1 second so windows has
  // overlapping data - same entry goes as last and first in two closest entries
  // 5> 2023-08-13T00:00:01Z to 2023-08-13T00:00:03Z : Deposit(Peggy,2023-08-13T00:00:01Z,9575594,Euro)Deposit(Peggy,2023-08-13T00:00:02Z,2525131,Euro)
  // 6> 2023-08-13T00:00:02Z to 2023-08-13T00:00:04Z : Deposit(Peggy,2023-08-13T00:00:02Z,2525131,Euro)Deposit(Charlie,2023-08-13T00:00:03Z,4103413,Yen)
}

class DepositBySlidingWindow extends AllWindowFunction[Deposit, String, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Deposit], out: Collector[String]): Unit =
    out.collect(
      s"${Instant.ofEpochMilli(window.getStart)} to ${Instant.ofEpochMilli(window.getEnd)} : ${input.mkString}"
    )
}
