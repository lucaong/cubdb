# Upgrading from v1 to v2

The database format is completely backward compatible, so version `v2` of
`CubDB` can load databases created with `v1`, and vice-versa. Upgrading from
`v1` to `v2` requires a few code changes though, as some functions changed
signature.

When calling `get_and_update`, `get_and_update_multi`, and `select` the return
value is not a `{:ok, result}` tuple anymore, but just `result`.

The function `get_and_update_multi` does not take a fourth option argument
anymore. The only available option was `:timeout`, which was now removed. In
case you want to enforce a timeout for the update function, you can use a `Task`
and `Task.yield` like explained
[here](https://hexdocs.pm/elixir/1.12/Task.html#yield/2).

The `select` function does not accept the `:pipe` and `:reduce` options anymore.
Instead, it returns a lazy stream that can be used with functions in the
`Stream` and `Enum` modules. For example:

```elixir
# This v1 code:
{:ok, product} =
  CubDB.select(db, [
    min_key: :foo,
    max_key: :bar,
    pipe: [
      map: fn {_, val} -> val end,
      filter: fn val -> val > 0 end
    ],
    reduce: fn val, acc -> val * acc end
  ])

# Can be rewritten to this code in v2:
product =
  CubDB.select(db, min_key: :foo, max_key: :bar)
  |> Stream.map(fn {_, val} -> val end)
  |> Stream.filter(fn val -> val > 0 end)
  |> Enum.reduce(fn val, acc -> val * acc end)
```

The `select` function also does not support a `:timeout` option anymore, so it
will be ignored if passed. In order to enforce a timeout, you can wrap the
`select` in a `Task` and use `Task.yield` like shown
[here](https://hexdocs.pm/elixir/1.12/Task.html#yield/2)
