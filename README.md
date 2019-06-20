# CubDB

`CubDB` is a pure-Elixir embedded key-value database, designed for simplicity.
It runs locally, and is backed by a single file.

Both keys and values can be any Elixir (or Erlang) term, so no serialization
and de-serialization is necessary.

The `CubDB` database file uses an immutable data structure that ensures
robustness to data corruption: entries are never changed in-place, and writes
are atomic.

Read operations are performed on immutable "snapshots", so they are always
consistent, run concurrently, and do not block write operations, nor are blocked
by them.

## Usage

Start `CubDB` by specifying a directory for its database file (if not existing,
it will be created):

```elixir
{:ok, db} = CubDB.start_link("my/data/directory")
```

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

Range of keys are retrieved using `select`:

```elixir
for {key, value} <- [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8] do
  CubDB.put(db, key, value)
end

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
  reverse: true,
  pipe: [
    map: fn {_key, value} ->
      value
    end,
    filter: fn value ->
      is_integer(value) && Integer.is_even(value)
    end,
    take: 3
  ],
  reduce: fn n, sum -> sum + n end
)
#=> {:ok, 18}
```

For more details, read the [API documentation](https://hexdocs.pm/cubdb/CubDB.html).

## Installation

`CubDB` can be installed by adding `cubdb` to your list of dependencies in
`mix.exs`:

```elixir
def deps do
  [{:cubdb, "~> 0.2.0"}]
end
```
