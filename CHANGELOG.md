# Changelog

For versions before `v1.0.0`, the changes are reported in the [GitHub
releases](https://github.com/lucaong/cubdb/releases).

Since `v1.0.0`, `CubDB` follows [semantic versioning](https://semver.org), and
reports changes here.

## v2.0.0 (unreleased, currently in release candidate state)

Version `2.0.0` brings better concurrency, atomic transactions with arbitrary
operations, zero cost read-only snapshots, database backup, and more, all with a
simpler and more scalable internal architecture.

Refer to the [upgrade guide](https://hexdocs.pm/cubdb/upgrading.html) for how to
upgrade from previous versions.

  - [breaking] The functions `CubDB.get_and_update/3`,
    `CubDB.get_and_update_multi/3`, and `CubDB.select/2` now return directly
    `result`, instead of a `{:ok, result}` tuple.
  - [breaking] `CubDB.get_and_update_multi/4` does not take an option argument
    anymore, making it `CubDB.get_and_update_multi/3`. The only available option
    used to be `:timeout`, which is not supported anymore.
  - [breaking] Remove the `:timeout` option on `CubDB.select/2`. This is part of
    a refactoring and improvement that moves read operations from an internally
    spawned `Task` to the client process. This makes the `:timeout` option
    unnecessary: by stopping the process calling `CubDB`, any running read
    operation by that process is stopped.
  - [breaking] `CubDB.select/2` now returns a lazy stream that can be used with
    functions in `Enum` and `Stream`. This makes the `:pipe` and `:reduce`
    options unnecessary, so those options were removed.
  - Add `CubDB.snapshot/2`, `CubDB.with_snapshot/2` and
    `CubDB.release_snapshot/1` to get zero cost read-only snapshots of the
    database. The functions in `CubDB.Snapshot` allow to read from a snapshot.
  - Add `CubDB.transaction/2` to perform multiple write (and read) operations in
    a single atomic transaction. The functions in `CubDB.Tx` allow to read and
    write inside a transaction.
  - Add `CubDB.back_up/2` to produce a database backup. The backup process does
    not block readers or writers, and is isolated from concurrent writes.
  - Add `CubDB.halt_compaction/1` to stop any running compaction operation
  - Add `CubDB.compacting?/1` to check if a compaction is currently running
  - Move read and write operations to the caller process as opposed to the
    `CubDB` server process.
  - Improve concurrency of read operations while writing

## v1.1.0 (2021-10-14)

  - Add `clear/1` function to atomically delete all entries in the database

## v1.0.0 (2021-06-24)

### Breaking changes from v0.17.0:

  - Better defaults:
    * `auto_file_sync` now defaults to `true` (slower but durable)
    * `auto_compact` now defaults to `true`
  - Functions `select/2` and `get_and_update_multi/4` now take the timeout as an option instead of an additional argument

### Other changes from v0.17.0:

  - Better internal handling of timeouts that ensures cleanup of resources on the callee side
  - Added `put_and_delete_multi/3` to atomically put and delete entries
  - Added `put_new/3` to put an entry only if the key does not exist yet
  - More efficient implementation of `put_multi/2` and `delete_multi/2`
  - Function `get_multi/2` does not block writers
  - Fix race condition during compaction
  - Function `get_and_update/3` avoids unnecessary disk writes (including the transaction header) when the value is unchanged
  - Remove caller timeout on `put_and_delete_multi/3` and `put_multi/2`, consistently with the other functions.
  - Remove default GenServer timeouts
  - Fix process (and file descriptor) leak upon compaction
  - Fix `cubdb_file?/1` regexp, making it stricter
