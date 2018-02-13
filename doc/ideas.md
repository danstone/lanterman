## GC / Delete

- if we remember old roots, (in a nested log) we can call
  (gc) to delete unreachable nodes from storage, or delete every node used by the
  log. You would need a logid shared between nodes
  so that storage nodes are unique and cannot be shared between logs if you
  want to be able to do any kind of deletion, otherwise you can
  break other logs in storage.

  This will stop certain kinds of operation, and break consumers that are looking
  at old log pointers. So it only works if you have a ref you can use to reacquire
  the old nodes.

  Need to think about this a bit more, if you do not allow GC many other things
  become simpler.

## Rewrites

- Certain operations may want to rewrite old portions of tree from
  left to right. Only one of these is really going to be performant
  under contention, as you do not want multiple threads rewriting the same
  nodes. Could be used to implement key compaction.

## Compaction

- compaction collection would be a log rewrite operation, but could
  rewrite tree segment by segment much like kafka.

## Slices

- You should be able to take a slice of a log in constant time, if the log
  is not collectable. If it is Gc'able you would need to register the slice somewhere?

## Append

- You should be able to append logs, and slices of logs in constant time
  as you can do a typical cat append.




