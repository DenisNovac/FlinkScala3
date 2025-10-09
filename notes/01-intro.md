# Flink Introduction

Apache Flink is a framework and distributed processing engine for stateful computations
over unbounded and bounded data streams.

- Stream processing - continuous processing, real time;
  - unbounded - no ending of stream;
  - bounded - ending, orders per one day;
    - freshness or completeness?;
- Stateful streaming based on multiple events happen on different time
  -  fault-tolerant, remembers the state, recovery.
- distributed system;
  - multiple machines working on same task;
  - clustering.

## Why Flink?

- Fault-tolerant;
  - Snapshots;
  - Every state of computation must be stored so operation could be restarted from it;
  - Manual snapshots - save point;
  - Automatic snapshots - checkpoint;
- Advanced event time handling;
  - Watermark - solution for out od ordered events;
  - injected to stream, timestamps of the latest event;
  - "up to this point results could be considered complete".
- Scalable;
  - Divide compute intensive jobs;
  - Allocate computing resources across task slots;
  - Keyed process, keys for distribution.

## Flink Lifecycle

Flink Cluster:
  - Job Manager (could be multiple, one master);
    - resources manager;
    - dispatcher (api, starts new job masters);
      - starting point of flink job;
    - job master (manage single job execution).
  - Task Manager;
    - execute the tasks of a data stream;
    - resource unit is called "task slot".
      - the more you have slots - the more parallelism you could have.

To run the job Submit the job (JAR file) to Flink Cluster via REST API: `flink run examples/MyJob.jar`.

To stop the job safely - you need to have a save point.

```bash
flink stop --savepointPath /tmp/flink $JOB_ID

flink run --detached --fromSavepoint /tmp/flink/savepoint-$JOB_ID examples/MyJob.jar
```









