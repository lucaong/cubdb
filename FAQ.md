# Frequently Asked Questions

## Why should I use CubDB?

If your Elixir application needs an embedded database (which is a database that
run _inside_ your application, as opposed to one that runs as a separate
software), then `CubDB` might be a great fit: its API is simple and idiomatic,
you can supervise it as any other Elixir process, and it runs wherever Elixir
runs, with no need to cross-compile native code. You can think about `CubDB` as
an Elixir collection, like maps or lists, but one that is stored to disk and
persistent to restarts.

The typical use-case for `CubDB` is data storage in single-instance applications
or embedded software (for example, `CubDB` is a great fit for
[Nerves](https://nerves-project.org) systems). In those contexts, `CubDB` is
typically used for things like:

  * Persisting configuration and preferences

  * Data logging and storage of metrics or time serieses

  * Local database for application data

## Why is CubDB better than X?

First of all, `CubDB` is not jealous: it does its job well, without claiming to
be better than others. There are many other great alternatives for storing data,
and depending on your use-case they might be a better fit.

Here are some reasons why you might choose `CubDB` over other popular
alternatives:

  * [ETS](http://erlang.org/doc/man/ets.html) (Erlang Term Store), like `CubDB`,
    can store arbitrary Elixir/Erlang terms, and comes by default with the
    Erlang runtime. Differently from `CubDB` though, ETS stores values in
    memory, so they are lost in case of a restart.

  * [DETS](http://erlang.org/doc/man/dets.html) is similar to ETS, but it
    persists values on disk. Compared to `CubDB` though, it does not support
    sorted collections. Also, you might find its API more convoluted that
    `CubDB`.

  * [Mnesia](http://erlang.org/doc/man/mnesia.html) is a distributed database
    system that comes with the Erlang runtime. It can be distributed, and
    enforces a schema. If on one hand it can make a great heavy-duty distributed
    database system, it is substantially more complicated to use than `CubDB`
    for embedded use cases.

  * [SQLite](https://www.sqlite.org/index.html),
    [LevelDB](https://github.com/google/leveldb),
    [LMDB](https://symas.com/lmdb/), etc. are all great general-purpose embedded
    databases. They support a variety of features, and have wider adoption than
    `CubDB`. Because they are not "Elixir native" though, you need a library to
    interact with them from Elixir, and they generally feel less "idiomatic"
    than `CubDB` when used from an Elixir application. In some cases (like when
    writing software for embedded devices), it can be more complicated to
    install and cross-compile them for your architecture, and some libraries
    might not play well with your supervision strategies (NIF libraries are
    fast, but will crash the Erlang VM if the native executable crash).

  * Plain files can be used to store simple data. Compared to `CubDB` though,
    they don't offer efficient key/value access or sorted collections, and they
    are subject to data corruption if a sudden power loss happens mid-write.
    `CubDB` is designed to be efficient and robust, so a power loss won't
    corrupt its data, or break atomicity.

If your use case is such that one of those alternatives is a better fit, by all
means go for it: `CubDB` won't be sad :) But if `CubDB` fits your use case, you
will enjoy its native Elixir feel and simple but versatile model.

## What is compaction?

`CubDB` uses an append-only Btree data structure: when you write to `CubDB`, the
change is appended to the data file, instead of modifying the existing data
in-place. This is efficient and robust: writing at the end of a file is faster
than "jumping around", and should something go wrong in the middle of a write
(say, a power failure), no data is destroyed by a partial overwrite, and `CubDB`
is able to recover upon restart.

The drawback of this approach though, is that the data file will keep growing as
you write to the database, even when you update or delete existing values.
Eventually, the data file will grow larger, as old values that are not
"reachable" anymore are still in. Compaction is the operation through which
`CubDB` "cleans up" and make its data file small and space-efficient again.
Other databases have similar processes (for example, SQLite calls it
"vacuuming").

During a compaction operation, `CubDB` creates a new file, and transfers to it
the current entries, without the stale data. When all the data is transferred,
including entries written after the compaction started, `CubDB` switches to use
the new compacted file and removes the old, obsolete one. The compaction process
is performed in the background, without blocking read or write operations on the
active data file.

Should a compaction operation be interrupted by a shutdown or an application
crash, no data is lost: the old data file is still up to date and active, and
upon the next compaction the file left over by the interrupted compaction is
removed and a new one created.

## Should I use auto compaction?

Usually, letting `CubDB` perform compaction automatically is the most convenient
choice: `CubDB` will keep track of its "dirt factor", which is an indicator of
how much overhead could be shaved off by a compaction, and clean up when
necessary.

In some situations though, it can be advisable to avoid auto compaction and
compact your database manually. One example is if you are performing a one-off
import of a lot of data: in this case, it makes sense to import all of it, and
manually trigger a compaction only afterwards. This reduces disk contention
during the import. Note that you can turn auto compaction on or off at runtime
with `CubDB.set_auto_compact/2`.

## What is file sync?

When you write to a file, your operative system will often buffer writes in
memory, and actually write them in the file system only later. This makes write
operations faster, because file system access is expensive, and buffering makes
it so several writes will be batched together in one single bigger write. The
drawback is that, should a power failure happen before the buffer is written to
the file system, data that was held in the buffer might be lost. When you want
to be sure that data is safe in the file system, you have to tell the operative
system to "file sync", which means to flush the buffer to disk.

With `CubDB`, you can chose to automatically sync each write operation, or to
manually sync when you prefer. If you need faster write performance, but you are
ok with a small chance of loosing recently written data in case of a power loss
(for example if you use `CubDB` to log large quantity of data), you might choose
to not perform automatic file sync. Note that the operative system will still
sync data periodically, and upon clean shutdown. If instead you want to be sure
that data that was successfully written by `CubDB` won't be lost, even in case
of a power failure, you should use the auto file sync option: write operations
will be sensibly slower, but each write will be safe on disk by the time the
write operation completes.
