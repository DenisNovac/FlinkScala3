# Timely streaming

- Processing time;
    - time when Flink processes event;
    - regardless when effect happen;
    - every run generates different format;
- Event time;
    - when event occurred;
    - consistent for every run;
    - latency occurs when waiting for late events;
        - how to control how much to wait until we push timely results?
        - use watermarks to control latency;
- Watermarks;
    - periodic markers with a timestamp in data stream that balance data latency and correctness;
    - default - every 2000 ms;
    - each watermark has timestamp;
    - all events before watermark considered processed;
    - watermarks stops emitting when stream is idle.

## Windows

With batch events it is easy to use aggregating functions. But
how do you group elements of infinite stream? Windows can split data stream in
a buckets of some size.

Time-based windows assign elements to windows based on time (processing/event times).

Window assigner - defines how elements are assigned to windows;

Common window assigners:

- tumbling windows (time);
    - windows with fixed size;
    - not overlap;
    - e.g. size: 5 seconds;
- sliding windows (time);
    - fixed size;
    - windows slide - how often do we move window;
    - could overlap if sliding time is smaller than window size;
        - e.g. we have small frequency of events producing but big frequency of windowing;
- session windows (time);
    - grouping by "sessions" of activity;
    - determined by session gaps - time gaps between events;
    - when no new element arrives for some time;
- global windows (not a time);
    - assigns same key to same window;
    - must specify a custom trigger to start computation.

Custom windows to make your own behaviour.

## Window lifecycle

- Creation: when first element arrives;
- Removal: when time passes max timestamp + allowed lateness.

Example: counting all the orders happened in one day ending 24:00. When should
the system produce the final count so the result is complete?

Allowed lateness - to control relevancy/speed problems. For example, we could
wait for 2 more minutes after midnight before sending the result to system.

Allowed lateness also allows to do sorting when we expect elements come out of order.

`WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))`

Why is it "Watermark" strategy? It is because late events will be after current
watermark by default so they won't get into the same window as other data. So watermark
with that strategy will wait a little and move if event with <10 seconds difference arrived.

## Watermark and partitions

Flink generates watermark periodically in all partitions. Overall watermarks -
minimum of all partitions watermarks.

## Window key

Windows could be keyed or non-keyed. Key should be defined before window definition.

Keys allows partitions, each partition has own windows.

Windows has functions:

- `ProcessWindowFunction`,
- `ReduceWindowFunction`,
- `AggregateWindowFunction`.

Window triggers - when window should start processing. E.g. when time passes or some count.
Custom triggers are possible.



