# Side Output

We can produce additional output from operators. Used in process function.

- Useful for splitting a stream of data without replicating the whole stream;
- number of output result stream can be one or many;
- type of data in the output stream can be different among side output streams and main stream.

```scala
val tag = OutputTag[String]("my side")

ctx.output(tag, "sideOutput1" + String.valueOf(userAge))

val sideOutputStream1: DataStram[String] =
  myMainStream
    .getSideOutput(outputTag)
```



