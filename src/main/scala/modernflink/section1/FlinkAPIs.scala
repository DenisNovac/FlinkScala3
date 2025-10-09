package modernflink.section1

import modernflink.model.UserAction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.*

/** Time, Action, UserId, Username
  *
  * 1688695496, Purchase, 36452, Clara
  *
  * 1688695497, Register, 14354, Maggie
  */

// --add-opens=java.base/java.util=ALL-UNNAMED
@main def flinkApis() = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val inputFile = env.readTextFile("src/main/resources/UserAction.txt")
  val userActionData: DataStream[UserAction] = inputFile.map(UserAction.fromString)

  // we want to answer a question - for each action which user has performed it

  def highLevel() = {
    // Lambda functions - high level, can't control details

    val userActionLambdaFunction =
      userActionData
        .keyBy(_.action)
        .map(data => (data.action, data.userid, data.name))

    userActionLambdaFunction.print()
    env.execute()
  }

  def lowLevel() = {
    // process functions - low level

    val userActionProcess =
      userActionData
        .keyBy(_.action)
        .process(new UserActionProcessFunction())

    userActionProcess.print()

    env.execute()
  }

  def richDemo() = {

    val userActionRich =
      userActionData
        .keyBy(_.action)
        .map(new RichMapFunction[UserAction, (String, String, String)] {
          override def map(value: UserAction): (String, String, String) =
            (value.action, value.userid, value.name)

          override def open(parameters: Configuration): Unit = {
            println("MapFunction processing starts")

            super.open(parameters)
          }

          override def close(): Unit = {
            println("MapFunction processing stops")

            super.close()
          }
        })

    userActionRich.print()

    env.execute()
  }

  // highLevel()

  // lowLevel()

  richDemo()
}

// Key, Data type, Output
class UserActionProcessFunction() extends KeyedProcessFunction[String, UserAction, (String, String, String)] {

  // context type is weird, but it is a class inside current KeyedProcessFunction
  // access to functionality of process function
  override def processElement(
      value: UserAction,
      ctx: KeyedProcessFunction[String, UserAction, (String, String, String)]#Context,
      out: Collector[(String, String, String)]
  ): Unit = {
    println(ctx.timerService().currentProcessingTime()) // we can see internal stream stuff
    println(ctx.getCurrentKey)

    out.collect((value.action, value.userid, value.name))
  }
}
