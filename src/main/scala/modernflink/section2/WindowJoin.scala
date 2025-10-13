package modernflink.section2

import modernflink.model.UserAction
import modernflink.model.PurchaseHistory
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.function.WindowFunction
import org.apache.flinkx.api.serializers.*

import java.time.Duration

// --add-opens=java.base/java.util=ALL-UNNAMED
@main def windowJoin() = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/UserAction.txt")
  val inputData: DataStream[UserAction] = inputFile.map(UserAction.fromString)

  val inputFile2 = env.readTextFile("src/main/resources/PurchaseDetail.txt")
  val inputData2: DataStream[PurchaseHistory] = inputFile2.map(PurchaseHistory.fromString)

  // when the uses made purchase and paid how much

  val userAction: DataStream[UserAction] =
    inputData
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[UserAction] {
            override def extractTimestamp(element: UserAction, recordTimestamp: Long): Long =
              element.timestamp
          })
      )

  val purchaseAction: DataStream[PurchaseHistory] =
    inputData2
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[PurchaseHistory] {
            override def extractTimestamp(element: PurchaseHistory, recordTimestamp: Long): Long =
              element.timestamp
          })
      )

  val windowJoin =
    userAction
      .join(purchaseAction)
      .where(userAction => userAction.userid) // join on u.userId = p.userId
      .equalTo(purchaseAction => purchaseAction.userid)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply((user, purchase) => s"${user.name} spent ${purchase.amount} USD at ${purchase.timestamp}")

  windowJoin.print()

  env.execute()
}
