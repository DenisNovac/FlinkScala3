package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flinkx.api.serializers.*
import java.time.{Duration, Instant}

@main def tumblingWindowDemo() =

  given instantTypeInfo: TypeInformation[Instant] =
    TypeInformation.of(classOf[Instant])

  val env = StreamExecutionEnvironment.getExecutionEnvironment
