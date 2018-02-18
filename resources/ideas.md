# Ideas

## Pluggable Storage

Log nodes should be storable in a generic way

Durable Ref will be used

extra useful features could be added like:

- unsafe-value (deletable/nameable value ref)
- byte count meta for better caches
- read/write to byte buffers for better streaming perf
- off heap direct memory
- other databases and storages, google cloud etc
- Better caching, e.g 500MB for durable ref caches, soft references etc.

## Extensible Buffers

Extensible automatic value serialization/deserialization, will require some  thought in terms of 
limited number of bits for type, e.g if byte is magic value, then we could dynamically
switch to extended buffer set. If buffer cannot be read then we can always return the 
bytes.

## LogPlus (GC, Retention)

LogPlus can combine a normal log and a garbage log that tracks old roots.
Instead of using normal value references it can use scoped references and safely delete old refs

LogPlus should always be hosted in a ref, does not make sense as a standalone.

You would also be able to delete from LogPlus as it can keep track of old refs in the garbage log and insert tombstones.

Retention should be easy as you can just chop log at offset and insert old refs into garbage log.
Would require timing data however, maybe on slab?

Garbage log itself can be GC'd after GC runs, by doing this you can eventually delete all garbage, you do not to insert
old garbage refs back into the garbage log, but eventually you will reach tail or at least smaller and smaller garbage logs.

Deletion requires 'ownership' of nodes in log, so you can only append new buffers/slabs if you share structure with other
logs it'd be bad bad bad.

## KeyedLog (LogPlus + Compaction)

If space is a concern a compactable log using keys should be doable. I would require this to be used only with bytes
to get people to think about key ordering, or to wrap in a buffer.

Process would perform batch deletions effectively rewriting log slowly deleting old refs.

## Streaming pre-fetch

When reading from a log we should always be preparing next slab etc, though probably policy for this is wise.

## Single Writer Multi Reader

I wonder whether rather than having topics you could shard a log as an atomic XFORM on key of messages
and get a log for each partition.

You could then subscribe to that partitioned set. This should be doable because of atomic commit without error.

## SubscriberCluster

A group of threads across machines who are allocated logs from a set.

## ZK

Zookeeper can be used to speed up both subscriptions and producers via leader election builtins.

## Java API

A built in AOT'd java API using Clojure RT should be easy enough to ship with lib so you can use it from java no probs.

## Lanterman Server

For non jvm clients or those who do not want to deal with ZK but get some automatic perf benefit.

## Pipelines

You should be able to define pipelines declaratively and get a high-performance job that will manage
batching etc for you.

## Log Stats

If we could calculate various message stats like how big messages are on average is etc we might be able to use that
data to be better optimize defaults.

## Log Constraints?

Refuse publishes based on set of declarative constraints. Probably not spec, maybe something based on data
as I think you would want the constraint schema to be part of the log itself so it needs to be serializable.

## Slab Off-Heap Caching

It would be good to be able to cache slabs with off-heap memory, redis, memcached etc.
