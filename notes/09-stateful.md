# Stateful streaming

When we need to know previous events to make something with new event. Flink need to
remember the past to process current event.

Rich functions support stateful operations.

Stateful streaming - operation that stores the information across several events over time
instead of just looking at one event at a time.

## Keyed State

- Flink can retain state on per-key basis;
- embedded key-value store;
- keys of parallelism = keys of states;
- states stored locally on each machine for fast retrieval of states.

## State backends

- Store states in data structure;
- store snapshot as part of a checkpoint by recording the key/value state at a point in time.

Types:

- hashmap state - heap (lost if cluster crash);
- Embedded RocksDB (in-memory but has periodic snapshots to disk).

The EmbeddedRocksDBStateBackend is encouraged for:

- Jobs with very large state, long windows, large key/value states.
- All high-availability setups.

## State types

- Value state (single value);
- List state (list of elements);
- Map state;
- Reducing state (single value represents aggregation of all values, e.g. sum);
- Aggregating state (single value, different from reduced because type could be 
not the same as values).

# Checkpoints

Snapshots are global and consistant. It stores not only location in data stream, but also
a state computed up to that point.

Global - we have multiple nodes who processes the job, each has own snapshots of their states.
However, Flink composes all states into one global state so no local states are conflict with
each-other.

When fail - the whole cluster will restart from the snapshot, not only the specific nodes.




