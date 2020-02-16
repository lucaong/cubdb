# How To

## Store multiple collections

The ability of `CubDB` to retrieve ranges of data sorted by key makes it ideal
to store sorted collections, sort of like tables in a relational database.
Different collections can be stored in the same database, or in separate ones.

The main reason to use separate `CubDB` databases is if you need different
configuration options for each database (like auto compaction or file sync).

Otherwise, it is usually simpler and more efficient to use a single database:
read operations in `CubDB` execute concurrently, and while write operations on
the same database are serialized, using multiple databases means losing the
advantage of the append-only nature of `CubDB`, resulting in often slower
patterns of file access.

One common way to store separate collections in the same database is to use
composite keys, usually tuples, leveraging the fact that the Erlang runtime
defines a [total ordering of all
terms](http://erlang.org/doc/reference_manual/expressions.html#term-comparisons).

Say that you want to store two collections: people and articles. You can
structure your keys to be tuples like `{:people, person_id}` for people, and
`{:articles, article_id}` for articles:

```elixir
# Add a few users:
:ok = CubDB.put(db, {:people, 1}, %{first_name: "Margaret", last_name: "Hamilton"})
:ok = CubDB.put(db, {:people, 2}, %{first_name: "Alan", last_name: "Turing"})

# Add a few articles
:ok = CubDB.put(db, {:articles, 1}, %{title: "Spaceship Guidance for dummies", text: "..."})
:ok = CubDB.put(db, {:articles, 2}, %{title: "Morphogenesis for the uninitiated", text: "..."})
```

We used numeric IDs in our example, but you can really use anything you want.

Getting a specific person or article by ID is trivial:

```elixir
person = CubDB.get(db, {:people, 1})

article = CubDB.get(db, {:articles, 2})
```

Selecting all members of a collection without selecting also other collections
can be easily done by leveraging the fact that tuples are compared element by
element. Therefore, here is how you can select all members of a specific
collection:

```elixir
# Select all people
{:ok, people} = CubDB.select(db, min_key: {:people, 0}, max_key: {:people, nil})

# Select all articles
{:ok, articles} = CubDB.select(db, min_key: {:articles, 0}, max_key: {:articles, nil})
```

This range selection works because `nil` is greater than all numbers, so `{:abc,
nil}` is greater than `{:abc, 123}`, but smaller than `{:bcd, :123}`.

## Save and restore a backup

`CubDB` stores its data in a single file with extension `.cub`, inside the
configured data directory. The filename is a hexadecimal value (containing only
lowercase letters from `a` to `f` and digits) and gets incremented by one upon
each compaction. Backing up a database is as simple as copying its current data
file (that can be found by calling `CubDB.current_db_file/1`). Note that, during
a compaction, a file with extension `.compact` is also created: you don't need
to copy that file for your backup, as the `.cub` file already contains all data.

To recover from a saved backup, it is sufficient to copy the backed-up `.cub`
file to a directory and start `CubDB` on that data directory. Make sure that no
other `.cub` file is present in the same directory, and that the filename is a
valid hexadecimal number: should more than one `.cub` files be present in the
same data directory, the one with the greatest hexadecimal value is used, and
the others are deleted upon the next compaction.
