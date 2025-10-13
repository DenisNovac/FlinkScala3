# Multiple streams

## Interval Join

Takes 2 stream - A and B with same key.

The interval join joins elements of two streams (we’ll call them A & B for now) with a
common key and where elements of stream B have timestamps that lie in a relative time
interval to timestamps of elements in stream A (e.g. +-30 seconds - events from Stream A and Stream B
need to be in one minute span to be correlated).

So stream A is leading here.

Join A and B if:

```
b.timestamp ∈ [a.timestamp + lowerBound; a.timestamp + upperBound]
```

Interval inclusive by default.

## Window Join

Joins elements with same key and belonging to same window.

Behaves like inner-join for events in the same window.

