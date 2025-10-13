# Combining streams

## Union

Simplest way to do - Union.

Data structure must be the same.

Example: Same data type temperatures from streams from multiple cities.

## Connect

Can handle different data types;

Can only merge two streams at a time.

```scala
streamA: DataStream[Int, String]
streamB: DataStream[String, String]

streamA.connect(streamB)
```

Needs to have coprocess function `coMap`, `coAlatMap` which transforms two elements into one.

