<p align="center"><img src="assets/cubdb_banner.png" width="80%"/></p>

[![Build Status](https://github.com/lucaong/cubdb/workflows/CI%20Build/badge.svg)](https://github.com/lucaong/cubdb/actions)
[![Coverage Status](https://coveralls.io/repos/github/lucaong/cubdb/badge.svg?branch=master&x=1)](https://coveralls.io/github/lucaong/cubdb?branch=master)
[![Module Version](https://img.shields.io/hexpm/v/cubdb.svg)](https://hex.pm/packages/cubdb)
[![Hex Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/cubdb/)
[![License](https://img.shields.io/hexpm/l/cubdb.svg)](https://github.com/lucaong/cubdb/blob/master/LICENSE)

`CubDB` is an embedded key-value database written in the Elixir language. It
runs locally, it is schema-less, and backed by a single file.

Head to the [API reference](https://hexdocs.pm/cubdb/CubDB.html) for usage
details, or read the [Frequently Asked
Questions](https://hexdocs.pm/cubdb/faq.html) and the [How To
section](https://hexdocs.pm/cubdb/howto.html) for more information.


## Features

  - Both keys and values can be any arbitrary Elixir (or Erlang) term.

  - Simple `get`, `put`, and `delete` operations

  - Arbitrary selection of ranges of entries sorted by key with `select`

  - Atomic transactions with `put_multi`, `get_and_update_multi`, etc.

  - Concurrent read operations, that do not block nor are blocked by writes

  - Zero cost read only snapshots

  - Unexpected shutdowns won't corrupt the database or break atomicity

  - Manual or automatic compaction to optimize space usage

To ensure consistency, performance, and robustness to data corruption, `CubDB`
database file uses an append-only, immutable B-tree data structure. Entries are
never changed in-place, and read operations are performed on immutable
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

Multiple operations can be performed as an atomic transaction with `put_multi`,
`delete_multi`, and the other `[...]_multi` functions:

```elixir
CubDB.put_multi(db, [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8])
#=> :ok
```

Range of entries sorted by key are retrieved using `select`:

```elixir
CubDB.select(db, min_key: :b, max_key: :e)
#=> {:ok, [b: 2, c: 3, d: 4, e: 5]}
```

But `select` can do much more than that. It can apply a pipeline of operations
(`map`, `filter`, `take`, `drop` and more) to the selected entries, it can
select the entries in normal or reverse order, and it can `reduce` the result
using an arbitrary function:

```elixir
# Take the sum of the last 3 even values:
CubDB.select(db,
  # select entries in reverse order
  reverse: true,

  # apply a pipeline of operations to the entries
  pipe: [
    # map each entry discarding the key and keeping only the value
    map: fn {_key, value} -> value end,

    # filter only even integers
    filter: fn value -> is_integer(value) && Integer.is_even(value) end,

    # take the first 3 values
    take: 3
  ],

  # reduce the result to a sum
  reduce: fn n, sum -> sum + n end
)
#=> {:ok, 18}
```

Zero cost snapshots are a useful feature when one needs to perform several reads
or selects, ensuring isolation from concurrent writes, but without blocking
writers. This is useful, for example, when reading imultiple keys depending on
each other. Snapshots come at no cost: nothing is actually copied or written on
disk or in memory, apart from some small bookkeeping:

```elixir
# the key of y depends on the value of x, so we ensure consistency by getting
# them from the same snapshot, isolating from the effects of concurrent writes
{x, y} = CubDB.with_snapshot(db, fn snap ->
  x = CubDB.get(snap, :x)
  y = CubDB.get(snap, x)

  {x, y}
end)
```

For more details, read the [API documentation](https://hexdocs.pm/cubdb/CubDB.html).

## Installation

`CubDB` can be installed by adding `:cubdb` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [
    {:cubdb, "~> 1.1.0"}
  ]
end
```

## Acknowledgement

The file data structure used by `CubDB` is inspired by
[CouchDB](http://couchdb.apache.org). A big thanks goes to the CouchDB
maintainers for the readable codebase and extensive documentation.

## Copyright and License

Copyright 2021 Luca Ongaro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
