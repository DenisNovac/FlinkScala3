package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.state.{CheckpointListener, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.serializers.*

import java.nio.file.Paths
import java.time.Instant

@main def checkpointDemo(): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // set checkpoint every 1 second
  env.enableCheckpointing(1000)

  // set checkpoint storage
  val checkpointPath = Paths.get("CheckpointStorage").toUri

  env.getCheckpointConfig
    .setCheckpointStorage(checkpointPath)

  val depositData = env
    .addSource(
      new DepositEventGenerator(
        sleepSeconds = 1,
        startTime = Instant.parse("2023-08-13T00:00:00.00Z")
      )
    )

  val countDepositStream =
    depositData
      .keyBy(_.currency)
      .process(new CountDepositCheckpoint)

  countDepositStream.print()

  env.execute()
}

class CountDepositCheckpoint
    extends KeyedProcessFunction[String, Deposit, String]
    with CheckpointedFunction
    with CheckpointListener {

  var depositStateCounter: ValueState[Int] = _

  // same function as before
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

  // called when snapshot for a checkpoint is requested
  override def snapshotState(context: FunctionSnapshotContext): Unit =
    println(s"Checkpoint at ${context.getCheckpointTimestamp}")

  override def initializeState(context: FunctionInitializationContext): Unit =
    depositStateCounter = context.getKeyedStateStore.getState(new ValueStateDescriptor[Int]("count", classOf[Int]))

  // let us know that checkpoint is complete
  override def notifyCheckpointComplete(checkpointId: Long): Unit = ()
}
