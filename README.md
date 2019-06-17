# CubDB

`CubDB` is a pure-Elixir embedded key-value database, designed to be simple to
use. It runs locally, and is backed by a file.

Both keys and values can be any Elixir (or Erlang) term, so no serialization
and de-serialization is necessary.

`CubDB` uses an immutable data structure that ensures robustness to data
corruption. Read and select operations are performed on immutable "snapshots",
so they are always consistent, run concurrently, and do not block write
operations, nor are blocked by them.

## Examples

Start `CubDB` by specifying a data directory for its file:

```elixir
{:ok, db} = CubDB.start_link("my/data/directory")
```

`get`, `put`, and `delete` operations work as you expect:

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
for {key, value} <- [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, e: 8] do
  CubDB.put(db, key, value)
end

CubDB.select(db, from_key: :b, to_key: :e)
#=> {:ok, [b: 2, c: 3, d: 4, e: 5]}
```

But `select` can much more than that:

```elixir
CubDB.select(db,
  from_key: :b,
  pipe: [
    map: fn {_key, value} -> value end,
    filter: fn n -> rem(n, 2) == 0 end,
    take: 3
  ],
  reduce: fn n, sum -> sum + n end
)
#=> {:ok, 12}
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `cubdb` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:cubdb, "~> 0.1.0"}]
end
```
