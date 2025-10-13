package modernflink.section2

import modernflink.model.{SubscriptionEvent, SubscriptionEventsGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.serializers.*

import java.time.{Duration, Instant}

@main def eventTimeExample = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val subscriptionGen = env.addSource(
    new SubscriptionEventsGenerator(
      sleepSeconds = 1,
      startTime = Instant.now()
    )
  )

  def processing = {
    subscriptionGen
      .keyBy(_.userId)
      // processing time - will use own time at the time of processing
      .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
      .apply(
        (
            userId,
            timeWindow,
            events,
            collector: Collector[(String, String)]
        ) => {
          collector.collect(
            userId,
            events
              .map(e =>
                s"Processing Time Window ${timeWindow.getStart} - ${timeWindow.getEnd}: ${e.getClass.getSimpleName}"
              )
              .mkString
          )
        }
      )
      .print()

    env.execute()
  }

  def eventTime = {

    val withWatermarks = subscriptionGen
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(10)) // how long we wait for late events
          .withTimestampAssigner(
            // extract time from the event rather than use processing time
            new SerializableTimestampAssigner[SubscriptionEvent] {
              override def extractTimestamp(element: SubscriptionEvent, recordTimestamp: Long): Long =
                element.time.toEpochMilli
            }
          )
      )

    withWatermarks
      .keyBy(_.userId)
      // processing time - will use own time at the time of processing
      .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
      .apply(
        (
            userId,
            timeWindow,
            events,
            collector: Collector[(String, String)]
        ) => {
          collector.collect(
            userId,
            events
              .map(e => s"Event Time Window ${timeWindow.getStart} - ${timeWindow.getEnd}: ${e.getClass.getSimpleName}")
              .mkString
          )
        }
      )
      .print()

    env.execute()

  }

  // tumbling

  eventTime

}
