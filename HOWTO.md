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
# Add a few people:
:ok = CubDB.put(db, {:people, 1}, %{first_name: "Margaret", last_name: "Hamilton"})
:ok = CubDB.put(db, {:people, 2}, %{first_name: "Alan", last_name: "Turing"})

# Add a few articles:
:ok = CubDB.put(db, {:articles, 1}, %{title: "Spaceship Guidance made easy", text: "..."})
:ok = CubDB.put(db, {:articles, 2}, %{title: "Morphogenesis for the uninitiated", text: "..."})
```

We used positive integers as IDs in our example, but you can really use anything
you want.

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
{:ok, people_wth_keys} = CubDB.select(db, min_key: {:people, 0}, max_key: {:people, nil})

# Select all articles
{:ok, articles_with_keys} = CubDB.select(db, min_key: {:articles, 0}, max_key: {:articles, nil})
```

This range selection works because our IDs are positive integers, and `nil` is
greater than all numbers, so `{:abc, nil}` is greater than `{:abc, 123}`, but
smaller than `{:bcd, :123}`.

## Save and restore a backup

Use the `CubDB.back_up/2` function to create a backup of the current state of
the database. Once the backup is completed, it can be opened by starting a new
`CubDB` process using the target directory of the backup as its data directory:

```elixir
# Backup the current state of the database
:ok = CubDB.back_up(db, "some/target/path")

# Open the backup as another CubDB process
{:ok, copy} = CubDB.start_link(data_dir: "some/target/path")
```
