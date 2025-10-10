package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flinkx.api.function.WindowFunction
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flinkx.api.serializers.*

import java.time.{Duration, Instant}

@main def tumblingWindowDemo() = {
  given instantTypeInfo: TypeInformation[Instant] =
    TypeInformation.of(classOf[Instant])

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val depositData =
    env
      .addSource(DepositEventGenerator(sleepSeconds = 1, startTime = Instant.now())) // generates every 1 second
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(2)) // wait 1 second
          .withTimestampAssigner(new SerializableTimestampAssigner[Deposit] {
            override def extractTimestamp(element: Deposit, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )

  // partition by currency so each currency in different windows
  // every 5 seconds (+ 1 second lateness) call processing function for given window
  // (just shows the whole window as a string)
  // 5 second windows of transactions
  val depositByWindowStream: WindowedStream[Deposit, String, TimeWindow] =
    depositData
      .keyBy[String](_.currency)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))

  val tumblingWindowsStream: DataStream[String] =
    depositByWindowStream
      .apply(new DepositByTumblingFunction)

  tumblingWindowsStream.print()

  env.execute()
}

class DepositByTumblingFunction extends WindowFunction[Deposit, String, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Deposit], out: Collector[String]): Unit = {
    out.collect(
      s"$key ${Instant.ofEpochMilli(window.getStart)} to ${Instant.ofEpochMilli(window.getEnd)}: ${input.mkString}"
    )
  }
}
