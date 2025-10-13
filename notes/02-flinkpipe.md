# Flink Pipeline

It consists of three parts:
  - data source;
    - file, database, kafka, etc...
    - bounded or unbounded;
  - transformation;
    - map/flatmap/etc;
    - parallel processing;
  - sink;
    - output.

Backpressure: if data produced faster than operator can process it 
pipeline slows down to accommodate the slowest operator.

Emits backpressure warning.

Parallel processing splitting a job across it's task slots.





