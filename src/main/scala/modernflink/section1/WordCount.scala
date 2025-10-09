package modernflink.section1

import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

@main def wordCount(): Unit = {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val text = env.fromElements(
    "Safer∙Sephiroth is the final boss in Final Fantasy VII",
    "fought at the end of \"The Planet's Judgment\".",
    "Sephiroth's final form, this is his most powerful transformation.",
    "This battle is penultimate, but is considered the final challenge of the game"
  )

  val wordCountOutput: DataStream[(String, Int)] =
    text
      .flatMap(_.toLowerCase.split("\\W+")) // split by spaces
      .map((_, 1)) // each word has 1 score
      .keyBy(_._1) // partitioning the stream by word
      .reduce((a, b) => (a._1, a._2 + b._2)) // sum the same words (a = b guaranteed by key_by)

  // wordCountOutput.print()

  // env.execute()

  wordCountOutput
    .executeAndCollect()
    .toVector
    .foreach(println)

  // rolling append - we can see each step in the stream
  // (battle,1)
  // (at,1)
  // (boss,1)
  // (planet,1)
  // (vii,1)
  // (fought,1)
  // (judgment,1)
  // (final,1)
  // (this,1)
  // (this,2)
  // (final,2)
  // (final,3)
  // (final,4)
  // (is,1)
  // (is,2)
  // (is,3)
  // (is,4)
  // (most,1)
  // (considered,1)
  // (the,1)
  // (the,2)
  // (s,1)
  // (s,2)
  // (his,1)
  // (powerful,1)
  // (the,3)
  // (the,4)
  // (game,1)
  // (safer,1)
  // (the,5)
  // (penultimate,1)
  // (form,1)
  // (but,1)
  // (challenge,1)
  // (end,1)
  // (of,1)
  // (sephiroth,1)
  // (transformation,1)
  // (of,2)
  // (sephiroth,2)
  // (in,1)
  // (fantasy,1)

  println()

  // we can re-run the stream twice
  wordCountOutput
    .executeAndCollect()
    .toVector
    .foreach(println)

}
