<p align="center"><img src="assets/cubdb_banner.png" width="80%"/></p>

[![Build Status](https://github.com/lucaong/cubdb/workflows/CI%20Build/badge.svg)](https://github.com/lucaong/cubdb/actions)
[![Coverage Status](https://coveralls.io/repos/github/lucaong/cubdb/badge.svg?branch=master&cachebust=3)](https://coveralls.io/github/lucaong/cubdb?branch=master)
[![Module Version](https://img.shields.io/hexpm/v/cubdb.svg)](https://hex.pm/packages/cubdb)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/cubdb/)
[![License](https://img.shields.io/hexpm/l/cubdb.svg)](https://github.com/lucaong/cubdb/blob/master/LICENSE)

`CubDB` is an embedded key-value database for the Elixir language. It is
designed for robustness, and for minimal need of resources.

Head to the [API reference](https://hexdocs.pm/cubdb/CubDB.html) for usage
details, or read the [Frequently Asked
Questions](https://hexdocs.pm/cubdb/faq.html) and the [How To
section](https://hexdocs.pm/cubdb/howto.html) for more information.


## Features

  - Both keys and values can be any Elixir (or Erlang) term.

  - Basic `get`, `put`, and `delete` operations, selection of ranges of entries
    sorted by key with `select`.

  - Atomic, Consistent, Isolated, Durable (ACID) transactions.

  - Multi version concurrency control (MVCC) allowing concurrent read
    operations, that do not block nor are blocked by writes.

  - Unexpected shutdowns or crashes won't corrupt the database or break
    atomicity of transactions.

  - Manual or automatic compaction to reclaim disk space.

To ensure consistency, performance, and robustness to data corruption, `CubDB`
database file uses an append-only, immutable B-tree data structure. Entries are
never changed in-place, and read operations are performed on zero cost immutable
snapshots.


## Usage

Start `CubDB` by specifying a directory for its database file (if not existing,
it will be created):

```elixir
{:ok, db} = CubDB.start_link(data_dir: "my/data/directory")
```

_Important: avoid starting multiple `CubDB` processes on the same data
directory. Only one `CubDB` process should use a specific data directory at any
time._

`get`, `put`, and `delete` operations work as you probably expect:

```elixir
CubDB.put(db, :foo, "some value")
#=> :ok

CubDB.get(db, :foo)
#=> "some value"

CubDB.delete(db, :foo)
#=> :ok

CubDB.get(db, :foo)
#=> nil
```

Multiple operations can be performed atomically with the `transaction` function
and the `CubDB.Tx` module:

```elixir
# Swapping `:a` and `:b` atomically:
CubDB.transaction(db, fn tx ->
  a = CubDB.Tx.get(tx, :a)
  b = CubDB.Tx.get(tx, :b)

  tx = CubDB.Tx.put(tx, :a, b)
  tx = CubDB.Tx.put(tx, :b, a)

  {:commit, tx, :ok}
end)
#=> :ok
```

Alternatively, it is possible to use `put_multi`, `delete_multi`, and the other
`[...]_multi` functions, which also guarantee atomicity:

```elixir
CubDB.put_multi(db, [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8])
#=> :ok
```

Range of entries sorted by key are retrieved using `select`:

```elixir
CubDB.select(db, min_key: :b, max_key: :e) |> Enum.to_list()
#=> [b: 2, c: 3, d: 4, e: 5]
```

The `select` function can select entries in normal or reverse order, and returns
a lazy stream, so one can use functions in the `Stream` and `Enum` modules to
map, filter, and transform the result, only fetching from the database the
relevant entries:

```elixir
# Take the sum of the last 3 even values:
CubDB.select(db, reverse: true) # select entries in reverse order
|> Stream.map(fn {_key, value} -> value end) # discard the key and keep only the value
|> Stream.filter(fn value -> is_integer(value) && Integer.is_even(value) end) # filter only even integers
|> Stream.take(3) # take the first 3 values
|> Enum.sum() # sum the values
#=> 18
```

Read-only snapshots are useful when one needs to perform several reads or
selects, ensuring isolation from concurrent writes, but without blocking them.
When nothing needs to be written, using a snapshot is preferable to using a
transaction, because it will not block writes.

Snapshots come at no cost: nothing is actually copied or written on disk or in
memory, apart from some small internal bookkeeping. After obtaining a snapshot
with `with_snapshot`, one can read from it using the functions in the
`CubDB.Snapshot` module:

```elixir
# the key of y depends on the value of x, so we ensure consistency by getting
# both entries from the same snapshot, isolating from the effects of concurrent
# writes
{x, y} = CubDB.with_snapshot(db, fn snap ->
  x = CubDB.Snapshot.get(snap, :x)
  y = CubDB.Snapshot.get(snap, x)

  {x, y}
end)
```

The functions that read multiple entries like `get_multi`, `select`, etc. are
internally using a snapshot, so they always ensure consistency and isolation
from concurrent writes, implementing multi version concurrency control (MVCC).

For more details, read the [API documentation](https://hexdocs.pm/cubdb/CubDB.html).

## Installation

`CubDB` can be installed by adding `:cubdb` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [
    {:cubdb, "~> 2.0.0"}
  ]
end
```

## Acknowledgement

The file data structure used by `CubDB` is inspired by
[CouchDB](http://couchdb.apache.org). A big thanks goes to the CouchDB
maintainers for the readable codebase and extensive documentation.

## Copyright and License

Copyright 2022 Luca Ongaro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
