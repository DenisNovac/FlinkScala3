package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}

import java.time.Instant
import scala.compiletime.uninitialized

@main def valueStateDemo(): Unit = {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val depositData: DataStream[Deposit] =
    env.addSource(
      new DepositEventGenerator(1, Instant.now())
    )

  val countDepositStream: DataStream[String] =
    depositData
      .keyBy(_.currency)
      .process(CountDeposit())

  // simpler way for value states
  val countDepositStream2: DataStream[String] =
    depositData
      .keyBy[String](_.currency)
      .mapWithState[String, Int] { case (deposit, state) =>
        (
          s"Total count of deposits for currency ${deposit.currency}: ${state.getOrElse(1)}",
          state.orElse(Some(1)).map(_ + 1) // return new state
        )
      }

  countDepositStream2.print()

  env.execute()
}

class CountDeposit() extends KeyedProcessFunction[String, Deposit, String] {

  var depositStateCounter: ValueState[Int] = uninitialized

  override def open(parameters: Configuration): Unit = {
    depositStateCounter = getRuntimeContext.getState(new ValueStateDescriptor[Int]("count state", classOf[Int]))
  }

  override def processElement(
      value: Deposit,
      ctx: KeyedProcessFunction[String, Deposit, String]#Context,
      out: Collector[String]
  ): Unit = {
    // set default value
    if depositStateCounter.value() == null.asInstanceOf[Int] then depositStateCounter.update(1)

    // get current state
    val curr = depositStateCounter.value()

    // update current state
    depositStateCounter.update(curr + 1)

    out.collect(s"Total count of deposits for currency ${value.currency}: $curr")
  }

}
