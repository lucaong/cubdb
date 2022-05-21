# Changelog

For versions before `v1.0.0`, the changes are reported in the [GitHub
releases](https://github.com/lucaong/cubdb/releases).

Since `v1.0.0`, `CubDB` follows [semantic versioning](https://semver.org), and
reports changes here.

## v2.0.0 (unreleased)

  - [breaking] The functions `get_and_update/3` and `get_and_update_multi/3` now
    return directly `result`, instead of a `{:ok, result}` tuple.
  - [breaking] `get_and_update_multi/4` does not take an option argument
    anymore, making it `get_and_update_multi/3`. The only available option used
    to be `:timeout`, which is not supported anymore.
  - [breaking] Remove the `:timeout` option on `select/2`. This is part of a
    refactoring and improvement that moves read operations from an internally
    spawned Task to the client process. This makes the `:timeout` option
    unnecessary: by stopping the process calling `CubDB`, any running read
    operation by that process is stopped.
  - Add `snapshot/2`, `with_snapshot/1` and `release_snapshot/1` to get zero
    cost read-only snapshots of the database. The functions in `CubDB.Snapshot`
    allow to read from a snapshot.
  - Add `transaction/2` to perform multiple write (and read) operations in a
    single atomic transaction. The functions in `CubDB.Tx` allow to read and
    write inside a transaction.
  - Add `back_up/2` to produce a database backup. The backup process does not
    block readers or writers, and is isolated from concurrent writes.
  - Add `halt_compaction/1` to stop any running compaction operation
  - Add `compacting?/1` to check if a compaction is currently running
  - Move read and write operations to the caller process as opposed to the
    `CubDB` server process.
  - Improve concurrency of read operations while writing

### Upgrading from v1 to v2

Upgrading from `v1` to `v2` requires a few simple code changes:

  1. When calling `get_and_update` or `get_and_update_multi`, the return value
     is not a `{:ok, result}` tuple anymore, but just `result`.
  2. `get_and_update_multi` does not take a fourth option argument anymore. The
     only available option was `:timeout`, which was now removed. In case you
     want to enforce a timeout for the update function, you can use a `Task` and
     `Task.yield` like explained
     [here](https://hexdocs.pm/elixir/1.12/Task.html#yield/2).
  3. The `select` function does not support a `:timeout` option anymore, so it
     will be ignored if passed. In order to enforce a timeout, you can wrap the
     `select` in a `Task` and use `Task.yield` like shown
     [here](https://hexdocs.pm/elixir/1.12/Task.html#yield/2)

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
