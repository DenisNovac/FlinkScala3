# Transformation

Three levels of API:
  - Table API/SQL - high-level analytics;
  - DataStream API - Stream and batch data processing (streams, windows);
  - Stateful Stream Processing - Stateful Event Driven Applications.

## Relational API

Comparable to SQL, declarative.

## DataStream API

Scala-like transformations (map, flatMap)

keyBy()
  - DataStream -> KeyedStream;
  - Does grouping or partitioning based on hash of key;
  - allows for parallel data flow;
  - expensive operations;
    - better to already have in source.

## Low-level API

### Process Function

Allows to access basic blocks of streaming app:
  - keyed state and timers

We need to override method:

```scala
dataStream
  .keyBy(_.id)
  .process(new MyProcessFunction)

class MyProcessFunction() extends KeyedProcessFunction[String, T, String] {

  override def processElement(
                               value: T,
                               ctx: KeyedProcessFunction[String, T, String]#Context,
                               out: util.Collector[String]
                             ): Unit =
    out.collect("My output data stream")
}
```

### Rich Function

Additional methods for lifecycle management on top of normal functions such as map.




