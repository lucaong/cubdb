defmodule CubDB do
  @moduledoc """
  `CubDB` is an embedded key-value database for the Elixir language. It is
  designed for robustness, and for minimal need of resources.

  ## Features

    - Both keys and values can be any Elixir (or Erlang) term.

    - Basic `get/3`, `put/3`, and `delete/2` operations, selection of ranges of
    entries sorted by key with `select/2`.

    - Atomic, Consistent, Isolated, Durable (ACID) transactions.

    - Multi version concurrency control (MVCC) allowing concurrent read
    operations, that do not block nor are blocked by writes.

    - Unexpected shutdowns or crashes won't corrupt the database or break
    atomicity of transactions.

    - Manual or automatic compaction to reclaim disk space.

  To ensure consistency, performance, and robustness to data corruption, `CubDB`
  database file uses an append-only, immutable B-tree data structure. Entries
  are never changed in-place, and read operations are performed on zero cost
  immutable snapshots.

  More information can be found in the following sections:

    - [Frequently Asked Questions](faq.html)
    - [How To](howto.html)

  ## Usage

  Start `CubDB` by specifying a directory for its database file (if not existing,
  it will be created):

      {:ok, db} = CubDB.start_link("my/data/directory")

  Alternatively, to specify more options, a keyword list can be passed:

      {:ok, db} = CubDB.start_link(data_dir: "my/data/directory", auto_compact: true)

  > #### Important {: .warning}
  >
  > Avoid starting multiple `CubDB` processes on the same data
  > directory. Only one `CubDB` process should use a specific data directory at any
  > time.

  `CubDB` functions can be called concurrently from different processes, but it
  is important that only one `CubDB` process is started on the same data
  directory.

  The `get/2`, `put/3`, and `delete/2` functions work as you probably expect:

      CubDB.put(db, :foo, "some value")
      #=> :ok

      CubDB.get(db, :foo)
      #=> "some value"

      CubDB.delete(db, :foo)
      #=> :ok

      CubDB.get(db, :foo)
      #=> nil

  Both keys and values can be any Elixir (or Erlang) term:

      CubDB.put(db, {"some", 'tuple', :key}, %{foo: "a map value"})
      #=> :ok

      CubDB.get(db, {"some", 'tuple', :key})
      #=> %{foo: "a map value"}


  Multiple operations can be performed atomically with the `transaction/2`
  function and the `CubDB.Tx` module:

      # Swapping `:a` and `:b` atomically:
      CubDB.transaction(db, fn tx ->
        a = CubDB.Tx.get(tx, :a)
        b = CubDB.Tx.get(tx, :b)

        tx = CubDB.Tx.put(tx, :a, b)
        tx = CubDB.Tx.put(tx, :b, a)

        {:commit, tx, :ok}
      end)
      #=> :ok

  Alternatively, it is possible to use `put_multi/2`, `delete_multi/2`, and the
  other `[...]_multi` functions, which also guarantee atomicity:

      CubDB.put_multi(db, [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8])
      #=> :ok

  Range of entries sorted by key are retrieved using `select/2`:

      CubDB.select(db, min_key: :b, max_key: :e) |> Enum.to_list()
      #=> [b: 2, c: 3, d: 4, e: 5]

  The `select/2` function can select entries in normal or reverse order, and returns
  a lazy stream, so one can use functions in the `Stream` and `Enum` modules to
  map, filter, and transform the result, only fetching from the database the
  relevant entries:

      # Take the sum of the last 3 even values:
      CubDB.select(db, reverse: true) # select entries in reverse order
      |> Stream.map(fn {_key, value} -> value end) # discard the key and keep only the value
      |> Stream.filter(fn value -> is_integer(value) && Integer.is_even(value) end) # filter only even integers
      |> Stream.take(3) # take the first 3 values
      |> Enum.sum() # sum the values
      #=> 18

  Read-only snapshots are useful when one needs to perform several reads or
  selects, ensuring isolation from concurrent writes, but without blocking them.
  When nothing needs to be written, using a snapshot is preferable to using a
  transaction, because it will not block writes.

  Snapshots come at no cost: nothing is actually copied or written on disk or in
  memory, apart from some small internal bookkeeping. After obtaining a snapshot
  with `with_snapshot/2`, one can read from it using the functions in the
  `CubDB.Snapshot` module:

      # the key of y depends on the value of x, so we ensure consistency by getting
      # both entries from the same snapshot, isolating from the effects of concurrent
      # writes
      {x, y} = CubDB.with_snapshot(db, fn snap ->
        x = CubDB.Snapshot.get(snap, :x)
        y = CubDB.Snapshot.get(snap, x)

        {x, y}
      end)

  The functions that read multiple entries like `get_multi/2`, `select/2`, etc.
  are internally using a snapshot, so they always ensure consistency and
  isolation from concurrent writes, implementing multi version concurrency
  control (MVCC).

  Because `CubDB` uses an immutable, append-only data structure, write
  operations cause the data file to grow. When necessary, `CubDB` runs a
  compaction operation to optimize the file size and reclaim disk space.
  Compaction runs in the background, without blocking other operations. By
  default, `CubDB` runs compaction automatically when necessary (see
  documentation of `set_auto_compact/2` for details). Alternatively, it can be
  started manually by calling `compact/1`.
  """

  @doc """
  Returns a specification to start this module under a supervisor.

  The default options listed in `Supervisor` are used.
  """
  use GenServer

  alias CubDB.Btree
  alias CubDB.CleanUp
  alias CubDB.Compactor
  alias CubDB.Reader
  alias CubDB.Snapshot
  alias CubDB.Store
  alias CubDB.Tx

  @db_file_extension ".cub"
  @compaction_file_extension ".compact"
  @known_file_extensions [@db_file_extension, @compaction_file_extension]
  @auto_compact_defaults {100, 0.25}

  @type key :: any
  @type value :: any
  @type entry :: {key, value}
  @type auto_compact :: {pos_integer, number} | boolean
  @type data_dir_option :: {:data_dir, String.t()}
  @type option :: {:auto_compact, auto_compact} | {:auto_file_sync, boolean}
  @type select_option ::
          {:min_key, any}
          | {:max_key, any}
          | {:min_key_inclusive, boolean}
          | {:max_key_inclusive, boolean}
          | {:reverse, boolean}

  @type server :: GenServer.server()

  defmodule State do
    @moduledoc false

    @type t :: %CubDB.State{
            btree: Btree.t(),
            data_dir: String.t(),
            task_supervisor: pid,
            clean_up: CleanUp.server(),
            compactor: pid | nil,
            compacting_store: Store.File.t() | nil,
            clean_up_pending: boolean,
            old_btrees: [Btree.t()],
            readers: %{required(reference) => String.t()},
            auto_compact: CubDB.auto_compact(),
            auto_file_sync: boolean,
            subs: list(pid),
            writer: GenServer.from() | nil,
            write_queue: :queue.queue()
          }

    @enforce_keys [:btree, :data_dir, :task_supervisor, :clean_up]
    defstruct @enforce_keys ++
                [
                  compactor: nil,
                  compacting_store: nil,
                  clean_up_pending: false,
                  old_btrees: [],
                  readers: %{},
                  auto_compact: true,
                  auto_file_sync: true,
                  subs: [],
                  writer: nil,
                  write_queue: :queue.new()
                ]
  end

  @spec start_link(
          String.t()
          | [option | data_dir_option | GenServer.option()]
        ) :: GenServer.on_start()

  @doc """
  Starts the `CubDB` database process linked to the current process.

  The argument is a keyword list of options:

    - `data_dir`: the directory path where the database files will be stored.
    This option is required. If the directory does not exist, it will be
    created. Only one `CubDB` instance can run per directory, so if you run
    several databases, they should each use their own separate data directory.

    - `auto_compact`: whether to perform compaction automatically. It defaults
    to `true`. See `set_auto_compact/2` for the possible values

    - `auto_file_sync`: whether to force flush the disk buffer on each write. It
    defaults to `true`. If set to `false`, write performance is faster, but
    durability of writes is not strictly guaranteed. See `set_auto_file_sync/2`
    for details.

  `GenServer` options like `name` and `timeout` can also be given, and are
  forwarded to `GenServer.start_link/3` as the third argument.

  If only the `data_dir` is specified, it is possible to pass it as a single
  string argument.

  ## Examples

      # Passing only the data dir
      {:ok, db} = CubDB.start_link("some/data/dir")

      # Passing data dir and other options
      {:ok, db} = CubDB.start_link(data_dir: "some/data/dir", auto_compact: true, name: :db)
  """
  def start_link(data_dir_or_options) do
    case split_options(data_dir_or_options) do
      {:ok, {data_dir, options, gen_server_options}} ->
        GenServer.start_link(
          __MODULE__,
          [data_dir, options, gen_server_options],
          gen_server_options
        )

      error ->
        error
    end
  end

  def start_link(data_dir, options) do
    start_link(Keyword.merge(options, data_dir: data_dir))
  end

  @spec start(String.t() | [option | data_dir_option | GenServer.option()]) ::
          GenServer.on_start()

  @doc """
  Starts the `CubDB` database without a link.

  See `start_link/2` for more information about options.
  """
  def start(data_dir_or_options) do
    case split_options(data_dir_or_options) do
      {:ok, {data_dir, options, gen_server_options}} ->
        GenServer.start(__MODULE__, [data_dir, options, gen_server_options], gen_server_options)

      error ->
        error
    end
  end

  def start(data_dir, options) do
    start(Keyword.merge(options, data_dir: data_dir))
  end

  @spec stop(server, term(), timeout()) :: :ok

  @doc """
  Synchronously stops the `CubDB` database.

  See `GenServer.stop/3` for details.
  """

  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(db, reason, timeout)
  end

  @spec get(server, key, value) :: value

  @doc """
  Gets the value associated to `key` from the database.

  If no value is associated with `key`, `default` is returned (which is `nil`,
  unless specified otherwise).
  """
  def get(db, key, default \\ nil) do
    with_snapshot(db, fn %Snapshot{btree: btree} ->
      Reader.get(btree, key, default)
    end)
  end

  @spec fetch(server, key) :: {:ok, value} | :error

  @doc """
  Fetches the value for the given `key` in the database, or returns `:error` if
  `key` is not present.

  If the database contains an entry with the given `key` and value `value`, it
  returns `{:ok, value}`. If `key` is not found, it returns `:error`.
  """
  def fetch(db, key) do
    with_snapshot(db, fn %Snapshot{btree: btree} ->
      Reader.fetch(btree, key)
    end)
  end

  @spec has_key?(server, key) :: boolean

  @doc """
  Returns whether an entry with the given `key` exists in the database.
  """
  def has_key?(db, key) do
    with_snapshot(db, fn %Snapshot{btree: btree} ->
      Reader.has_key?(btree, key)
    end)
  end

  @spec select(server, [select_option]) :: Enumerable.t()

  @doc """
  Selects a range of entries from the database, returning a lazy stream.

  The returned lazy stream can be filtered, mapped, and transformed with
  standard functions in the `Stream` and `Enum` modules. The actual database
  read is deferred to when the stream is iterated or evaluated.

  ## Options

  The `min_key` and `max_key` specify the range of entries that are selected. By
  default, the range is inclusive, so all entries that have a key greater or
  equal than `min_key` and less or equal then `max_key` are selected:

      # Select all entries where "a" <= key <= "d"
      CubDB.select(db, min_key: "b", max_key: "d")

  The range boundaries can be excluded by setting `min_key_inclusive` or
  `max_key_inclusive` to `false`:

      # Select all entries where "a" <= key < "d"
      CubDB.select(db, min_key: "b", max_key: "d", max_key_inclusive: false)

  Any of `:min_key` and `:max_key` can be omitted, to leave the range
  open-ended.

      # Select entries where key <= "a"
      CubDB.select(db, max_key: "a")

  Since `nil` is a valid key, setting `min_key` or `max_key` to `nil` *does NOT*
  leave the range open ended:

      # Select entries where nil <= key <= "a"
      CubDB.select(db, min_key: nil, max_key: "a")

  The `reverse` option, when set to true, causes the entries to be selected and
  traversed in reverse order. This is more efficient than selecting them in
  normal ascending order and then reversing the resulting collection.

  Note that, when selecting a key range, specifying `min_key` and/or `max_key`
  is more performant than using functions in `Enum` or `Stream` to filter out
  entries out of range, because `min_key` and `max_key` avoid loading
  unnecessary entries from disk entirely.

  ## Examples

  To select all entries with keys between `:a` and `:c` as a stream of `{key,
  value}` entries we can do:

      entries = CubDB.select(db, min_key: :a, max_key: :c)

  Since `select/2` returns a lazy stream, at this point nothing has been fetched
  from the database yet. We can turn the stream into a list, performing the
  actual query:

      Enum.to_list(entries)

  If we want to get all entries with keys between `:a` and `:c`, with `:c`
  excluded, we can do:

      entries =
        CubDB.select(db,
          min_key: :a,
          max_key: :c,
          max_key_inclusive: false
        )
        |> Enum.to_list()

  To select the last 3 entries, we can do:

      entries = CubDB.select(db, reverse: true) |> Enum.take(3)

  If we want to obtain the sum of the first 10 positive numeric values
  associated to keys from `:a` to `:f`, we can do:

      sum =
        CubDB.select(db,
          min_key: :a,
          max_key: :f
        )
        |> Stream.map(fn {_key, value} -> value end) # map values
        |> Stream.filter(fn n -> is_number(n) and n > 0 end) # only positive numbers
        |> Stream.take(10) # take only the first 10 entries in the range
        |> Enum.sum() # sum the selected values

  Using functions from the `Stream` module for mapping and filtering ensures
  that we do not fetch unnecessary items from the database. In the example
  above, for example, after fetching the first 10 entries satisfying the filter
  condition, no further entry is fetched from the database.
  """
  def select(db, options \\ []) when is_list(options) do
    Stream.resource(
      fn ->
        snap = CubDB.snapshot(db, :infinity)
        %Snapshot{btree: btree} = snap
        stream = Reader.select(btree, options)
        step = fn val, _acc -> {:suspend, val} end
        next = &Enumerable.reduce(stream, &1, step)
        {snap, next}
      end,
      fn {snap, next} ->
        case next.({:cont, nil}) do
          {:done, _} ->
            {:halt, {snap, nil}}

          {:suspended, value, next} ->
            {[value], {snap, next}}
        end
      end,
      fn {snap, _} -> CubDB.release_snapshot(snap) end
    )
  end

  @spec size(server) :: non_neg_integer

  @doc """
  Returns the number of entries present in the database.
  """
  def size(db) do
    with_snapshot(db, fn %Snapshot{btree: btree} ->
      Reader.size(btree)
    end)
  end

  @spec snapshot(server, timeout) :: Snapshot.t()

  @doc """
  Returns a snapshot of the database in its current state.

  > #### Note {: .neutral}
  >
  >  It is usually better to use `with_snapshot/2` instead of `snapshot/2`,
  > as the former automatically manages the snapshot life cycle, even in case of
  > crashes.

  A snapshot is an immutable, read-only representation of the database at a
  specific point in time. Getting a snapshot is basically zero-cost: nothing
  needs to be copied or written, apart from some small in-memory bookkeeping.

  The only cost of a snapshot is that it delays cleanup of old files after
  compaction for as long as it is in use. For this reason, a snapshot has a
  timeout, configurable as the optional second argument of `snapshot/2`
  (defaulting to 5000, or 5 seconds, if not specified). After such timeout
  elapses, the snapshot cannot be used anymore, and any pending cleanup is
  performed.

  It is possible to pass `:infinity` as the timeout, but then one must manually
  call `release_snapshot/1` to release the snapshot after use.

  Using `with_snapshot/1` is often a better alternative to `snapshot/2`, as it
  does not require to choose an arbitrary timeout, and automatically ensures
  that the the snapshot is released after use, even in case of a crash.

  After obtaining a snapshot, it is possible to read from it using the functions
  in `CubDB.Snapshot`, which work the same way as the functions in `CubDB` with
  the same name, such as `CubDB.Snapshot.get/3`, `CubDB.Snapshot.get_multi/2`,
  `CubDB.Snapshot.fetch/2`, `CubDB.Snapshot.has_key?/2`,
  `CubDB.Snapshot.select/2`.

  It is *not* possible to write on a snapshot.

  ## Example

      CubDB.put(db, :a, 123)
      snap = CubDB.snapshot(db)

      CubDB.put(db, :a, 0)

      # Getting a value from the snapshot returns the value of the entry at the
      # time the snapshot was obtained, even if the entry has changed in the
      # meanwhile
      CubDB.Snapshot.get(snap, :a)
      # => 123

      # Getting the same value from the database returns the latest value
      CubDB.get(db, :a)
      # => 0
  """
  def snapshot(db, timeout \\ 5000) do
    GenServer.call(db, {:snapshot, timeout}, :infinity)
  end

  @spec release_snapshot(Snapshot.t()) :: :ok

  @doc """
  Releases a snapshot when it is not needed anymore, releasing related resources

  This allows `CubDB` to perform cleanup operations after compaction that are
  otherwise blocked by the snapshot. When creating a snapshot with a timeout, it
  is not necessary to call `release_snapshot/1`, as it will be automatically
  released after the timeout elapses. When getting a snapshot with a timeout of
  `:infinity` though, one has to manually call `release_snapshot/1` once the
  snapshot is not needed anymore.

  In most cases, using `with_snapshot/2` is a better alternative to manually
  calling `snapshot/2` and `release_snapshot/1`
  """
  def release_snapshot(snapshot) do
    %Snapshot{db: db} = snapshot
    GenServer.call(db, {:release_snapshot, snapshot}, :infinity)
  end

  @spec with_snapshot(server, (Snapshot.t() -> result)) :: result when result: any

  @doc """
  Calls `fun` passing a snapshot, and automatically releases the snapshot when
  the function returns

  It returns the value returned by the function `fun`.

  A snapshot is an immutable, read-only representation of the database at a
  specific point in time, isolated from writes. It is basically zero-cost:
  nothing needs to be copied or written, apart from some small in-memory
  bookkeeping.

  Calling `with_snapshot/2` is equivalent to obtaining a snapshot with
  `snapshot/2` using a timeout of `:infinity`, calling `fun`, then manually
  releasing the snapshot with `release_snapshot/1`, but `with_snapshot/2`
  automatically manages the snapshot life cycle, also in case an exception is
  raised, a value is thrown, or the process exists. This makes `with_snapshot/2`
  usually a better choice than `snapshot/2`.

  After obtaining a snapshot, it is possible to read from it using the functions
  in `CubDB.Snapshot`, which work the same way as the functions in `CubDB` with
  the same name, such as `CubDB.Snapshot.get/3`, `CubDB.Snapshot.get_multi/2`,
  `CubDB.Snapshot.fetch/2`, `CubDB.Snapshot.has_key?/2`,
  `CubDB.Snapshot.select/2`.

  It is *not* possible to write on a snapshot.

  ## Example

  Assume that we have two entries in the database, and the key of the second
  entry depends on the value of the first (so the value of the first entry
  "points" to the other entry). In this case, we want to get both entries from
  the same snapshot, to avoid inconsistencies due to concurrent writes. Here's
  how that can be done with `with_snapshot/2`:

      {x, y} = CubDB.with_snapshot(db, fn snap ->
        x = CubDB.Snapshot.get(snap, :x)
        y = CubDB.Snapshot.get(snap, x)
        {x, y}
      end)
  """
  def with_snapshot(db, fun) do
    snap = snapshot(db, :infinity)

    try do
      fun.(snap)
    after
      release_snapshot(snap)
    end
  end

  @spec transaction(server, (Tx.t() -> {:commit, Tx.t(), result} | {:cancel, result})) ::
          result
        when result: any

  @doc """
  Starts a write transaction, passes it to the given function, and commits or
  cancels it depending on the return value.

  The transaction blocks other writers until the function returns, but does not
  block concurrent readers. When the need is to only read inside a transaction,
  and not perform any write, using a snapshot is a better choice, as it does not
  block writers (see `with_snapshot/2`).

  The module `CubDB.Tx` contains functions to perform read and write operations
  within the transaction. The function `fun` is called with the transaction as
  argument, and should return `{:commit, tx, results}` to commit the transaction
  `tx` and return `result`, or `{:cancel, result}` to cancel the transaction and
  return `result`.

  If an exception is raised, or a value thrown, or the process exits while
  inside of a transaction, the transaction is cancelled.

  Only use `CubDB.Tx` functions to write when inside a transaction (like
  `CubDB.Tx.put/3` or `CubDB.Tx.delete/2`). Using functions in the `CubDB`
  module to perform a write when inside a transaction (like `CubDB.put/3` or
  `CubDB.delete/2`) raises an exception. Note that write functions in `CubDB.Tx`
  have a functional API: they return a modified transaction rather than mutating
  it in place.

  The transaction value passed to `fun` should not be used outside of the
  function.

  ## Example:

  Suppose the keys `:a` and `:b` map to balances, and we want to transfer 5 from
  `:a` to `:b`, if `:a` has enough balance:

      CubDB.transaction(db, fn tx ->
        a = CubDB.Tx.get(tx, :a)
        b = CubDB.Tx.get(tx, :b)

        if a >= 5 do
          tx = CubDB.Tx.put(tx, :a, a - 5)
          tx = CubDB.Tx.put(tx, :b, b + 5)
          {:commit, tx, :ok}
        else
          {:cancel, :not_enough_balance}
        end
      end)

  The read functions in `CubDB.Tx` read the in-transaction state, as opposed to
  the live database state, so they see writes performed inside the transaction
  even before they are committed:

      # Assuming we start from an empty database
      CubDB.transaction(db, fn tx ->
        tx = CubDB.Tx.put(tx, :a, 123)

        # CubDB.Tx.get sees the in-transaction value
        CubDB.Tx.get(tx, :a)
        # => 123

        # CubDB.get instead does not see the uncommitted write
        CubDB.get(db, :a)
        # => nil

        {:commit, tx, nil}
      end)

      # After the transaction is committed, CubDB.get sees the write
      CubDB.get(db, :a)
      # => 123
  """
  def transaction(db, fun) do
    tx = start_transaction(db)

    returned =
      try do
        fun.(tx)
      rescue
        exception ->
          cancel_transaction(db)
          reraise(exception, __STACKTRACE__)
      catch
        :throw, value ->
          cancel_transaction(db)
          throw(value)

        :exit, value ->
          cancel_transaction(db)
          exit(value)
      end

    case returned do
      {:commit, %Tx{} = tx, result} ->
        commit_transaction(db, tx)
        result

      {:cancel, result} ->
        cancel_transaction(db)
        result

      _ ->
        raise "Wrong return value from CubDB.transaction/2 function, only {:commit, transaction, result} or {:cancel, result} are allowed"
    end
  end

  @spec start_transaction(server) :: Tx.t()

  defp start_transaction(db) do
    case GenServer.call(db, :start_transaction, :infinity) do
      {:error, :already_in_transaction} ->
        raise "Cannot start nested write transaction. You might be using CubDB instead of CubDB.Tx to perform writes inside of a transaction"

      %Tx{} = tx ->
        tx
    end
  end

  @spec cancel_transaction(server) :: :ok

  defp cancel_transaction(db) do
    GenServer.call(db, :cancel_transaction, :infinity)
  end

  @spec commit_transaction(server, Tx.t()) :: :ok

  defp commit_transaction(db, tx) do
    case GenServer.call(db, {:commit_transaction, tx}, :infinity) do
      {:error, :invalid_owner} ->
        raise "Attempt to commit a transaction started by a different owner"

      :ok ->
        :ok
    end
  end

  @spec dirt_factor(server) :: float

  @doc """
  Returns the dirt factor.

  The dirt factor is a number, ranging from 0 to 1, giving an indication about
  the amount of overhead disk space (or "dirt") that can be cleaned up with a
  compaction operation. A value of 0 means that there is no overhead, so a
  compaction would have no benefit. The closer to 1 the dirt factor is, the more
  can be cleaned up in a compaction operation.
  """
  def dirt_factor(db) do
    GenServer.call(db, :dirt_factor, :infinity)
  end

  @spec put(server, key, value) :: :ok

  @doc """
  Writes an entry in the database, associating `key` to `value`.

  If `key` was already present, it is overwritten.
  """
  def put(db, key, value) do
    transaction(db, fn tx ->
      {:commit, Tx.put(tx, key, value), :ok}
    end)
  end

  @spec put_new(server, key, value) :: :ok | {:error, :exists}

  @doc """
  Writes an entry in the database, associating `key` to `value`, only if `key`
  is not yet in the database.

  If `key` is already present, it does not change it, and returns `{:error,
  :exists}`.
  """
  def put_new(db, key, value) do
    transaction(db, fn tx ->
      case Tx.put_new(tx, key, value) do
        {:error, :exists} = reply ->
          {:cancel, reply}

        tx ->
          {:commit, tx, :ok}
      end
    end)
  end

  @spec delete(server, key) :: :ok

  @doc """
  Deletes the entry associated to `key` from the database.

  If `key` was not present in the database, nothing is done.
  """
  def delete(db, key) do
    transaction(db, fn tx ->
      {:commit, Tx.delete(tx, key), :ok}
    end)
  end

  @spec update(server, key, value, (value -> value)) :: :ok

  @doc """
  Updates the entry corresponding to `key` using the given function.

  If `key` is present in the database, `fun` is invoked with the corresponding
  `value`, and the result is set as the new value of `key`. If `key` is not
  found, `initial` is inserted as the value of `key`.
  """
  def update(db, key, initial, fun) do
    get_and_update_multi(db, [key], fn entries ->
      case Map.fetch(entries, key) do
        :error ->
          {:ok, %{key => initial}, []}

        {:ok, value} ->
          {:ok, %{key => fun.(value)}, []}
      end
    end)
  end

  @spec get_and_update(server, key, (value -> {any, value} | :pop)) :: any

  @doc """
  Gets the value corresponding to `key` and updates it, in one atomic transaction.

  `fun` is called with the current value associated to `key` (or `nil` if not
  present), and must return a two element tuple: the result value to be
  returned, and the new value to be associated to `key`. `fun` may also return
  `:pop`, in which case the current value is deleted and returned.

  Note that in case the value to update returned by `fun` is the same as the
  original value, no write is performed to disk.
  """
  def get_and_update(db, key, fun) do
    get_and_update_multi(db, [key], fn entries ->
      value = Map.get(entries, key, nil)

      case fun.(value) do
        {result, ^value} -> {result, [], []}
        {result, new_value} -> {result, %{key => new_value}, []}
        :pop -> {value, [], [key]}
      end
    end)
  end

  @spec get_and_update_multi(
          server,
          [key],
          (%{optional(key) => value} -> {any, %{optional(key) => value} | nil, [key] | nil})
        ) :: any

  @doc """
  Gets and updates or deletes multiple entries in an atomic transaction.

  Gets all values associated with keys in `keys_to_get`, and passes them as a
  map of `%{key => value}` entries to `fun`. If a key is not found, it won't be
  added to the map passed to `fun`. Updates the database and returns a result
  according to the return value of `fun`.

  The function `fun` should return a tuple of three elements: `{return_value,
  entries_to_put, keys_to_delete}`, where `return_value` is an arbitrary value
  to be returned, `entries_to_put` is a map of `%{key => value}` entries to be
  written to the database, and `keys_to_delete` is a list of keys to be deleted.

  The read and write operations are executed as an atomic transaction, so they
  will either all succeed, or all fail. Note that `get_and_update_multi/3`
  blocks other write operations until it completes.

  ## Example

  Assuming a database of names as keys, and integer monetary balances as values,
  and we want to transfer 10 units from `"Anna"` to `"Joy"`, returning their
  updated balance:

      {anna, joy} = CubDB.get_and_update_multi(db, ["Anna", "Joy"], fn entries ->
        anna = Map.get(entries, "Anna", 0)
        joy = Map.get(entries, "Joy", 0)

        if anna < 10, do: raise(RuntimeError, message: "Anna's balance is too low")

        anna = anna - 10
        joy = joy + 10

        {{anna, joy}, %{"Anna" => anna, "Joy" => joy}, []}
      end)

  Or, if we want to transfer all of the balance from `"Anna"` to `"Joy"`,
  deleting `"Anna"`'s entry, and returning `"Joy"`'s resulting balance:

      joy = CubDB.get_and_update_multi(db, ["Anna", "Joy"], fn entries ->
        anna = Map.get(entries, "Anna", 0)
        joy = Map.get(entries, "Joy", 0)

        joy = joy + anna

        {joy, %{"Joy" => joy}, ["Anna"]}
      end)
  """
  def get_and_update_multi(db, keys_to_get, fun) do
    transaction(db, fn %Tx{btree: btree} = tx ->
      key_values = Reader.get_multi(btree, keys_to_get)
      {result, entries_to_put, keys_to_delete} = fun.(key_values)

      case do_put_and_delete_multi(tx, entries_to_put, keys_to_delete) do
        {:cancel, :ok} ->
          {:cancel, result}

        {:commit, tx, :ok} ->
          {:commit, tx, result}
      end
    end)
  end

  @spec put_and_delete_multi(server, %{key => value}, [key]) :: :ok

  @doc """
  Writes and deletes multiple entries all at once, atomically.

  Entries to put are passed as a map of `%{key => value}` or a list of `{key,
  value}`. Keys to delete are passed as a list of keys.
  """
  def put_and_delete_multi(db, entries_to_put, keys_to_delete) do
    transaction(db, fn tx ->
      do_put_and_delete_multi(tx, entries_to_put, keys_to_delete)
    end)
  end

  @spec do_put_and_delete_multi(Tx.t(), [entry], [key]) ::
          {:commit, Tx.t(), :ok} | {:cancel, :ok}

  defp do_put_and_delete_multi(_tx, [], []), do: {:cancel, :ok}

  defp do_put_and_delete_multi(_tx, entries_to_put, []) when entries_to_put == %{},
    do: {:cancel, :ok}

  defp do_put_and_delete_multi(tx, entries_to_put, keys_to_delete) do
    tx =
      Enum.reduce(entries_to_put || [], tx, fn {key, value}, tx ->
        Tx.put(tx, key, value)
      end)

    tx =
      Enum.reduce(keys_to_delete || [], tx, fn key, tx ->
        Tx.delete(tx, key)
      end)

    {:commit, tx, :ok}
  end

  @spec get_multi(server, [key]) :: %{key => value}

  @doc """
  Gets multiple entries corresponding by the given keys all at once, atomically.

  The keys to get are passed as a list. The result is a map of key/value entries
  corresponding to the given keys. Keys that are not present in the database
  won't be in the result map.

  ## Example

      CubDB.put_multi(db, a: 1, b: 2, c: nil)

      CubDB.get_multi(db, [:a, :b, :c, :x])
      # => %{a: 1, b: 2, c: nil}
  """
  def get_multi(db, keys) do
    with_snapshot(db, fn %Snapshot{btree: btree} ->
      Reader.get_multi(btree, keys)
    end)
  end

  @spec put_multi(server, %{key => value} | [entry]) :: :ok

  @doc """
  Writes multiple entries all at once, atomically.

  Entries are passed as a map of `%{key => value}` or a list of `{key, value}`.
  """
  def put_multi(db, entries) do
    put_and_delete_multi(db, entries, [])
  end

  @spec delete_multi(server, [key]) :: :ok

  @doc """
  Deletes multiple entries corresponding to the given keys all at once, atomically.

  The `keys` to be deleted are passed as a list.
  """
  def delete_multi(db, keys) do
    put_and_delete_multi(db, %{}, keys)
  end

  @spec clear(server) :: :ok

  @doc """
  Deletes all entries, resulting in an empty database.

  The deletion is atomic, and is much more performant than deleating each entry
  manually.

  The operation respects all the guarantees of consistency of other concurrent
  operations. For example, if `select\2` was called before the call to `clear/1`
  and is running concurrently, the `select\2` will still see all the entries.

  If a compaction is in progress when `clear/1` is called, the compaction is
  halted, and a new one started immediately after. The new compaction should be
  very fast, as the database is empty as a result of the `clear/1` call.
  """
  def clear(db) do
    transaction(db, fn tx ->
      {:commit, Tx.clear(tx), :ok}
    end)
  end

  @spec compact(server) :: :ok | {:error, :pending_compaction | term}

  @doc """
  Runs a database compaction.

  As write operations are performed on a database, its file grows. Occasionally,
  a compaction operation can be run to shrink the file to its optimal size.
  Compaction runs in the background and does not block operations.

  Only one compaction operation can run at any time, therefore if this function
  is called when a compaction is already running, it returns `{:error,
  :pending_compaction}`.

  When compacting, `CubDB` will create a new data file, and eventually switch to
  it and remove the old one as the compaction succeeds. For this reason, during
  a compaction, there should be enough disk space for a second copy of the
  database file.

  Compaction can create disk contention, so it should not be performed
  unnecessarily often.
  """
  def compact(db) do
    GenServer.call(db, :compact, :infinity)
  end

  @spec set_auto_compact(server, boolean | {integer, integer | float}) ::
          :ok | {:error, String.t()}

  @doc """
  Configures whether to perform automatic compaction, and how.

  If set to `false`, no automatic compaction is performed. If set to `true`,
  auto-compaction is performed, following a write operation, if at least 100
  write operations occurred since the last compaction, and the dirt factor is at
  least 0.25. These values can be customized by setting the `auto_compact`
  option to `{min_writes, min_dirt_factor}`.

  It returns `:ok`, or `{:error, reason}` if `setting` is invalid.

  Compaction is performed in the background and does not block other operations,
  but can create disk contention, so it should not be performed unnecessarily
  often. When writing a lot into the database, such as when importing data from
  an external source, it is advisable to turn off auto compaction, and manually
  run compaction at the end of the import.
  """
  def set_auto_compact(db, setting) do
    GenServer.call(db, {:set_auto_compact, setting}, :infinity)
  end

  @spec halt_compaction(server) :: :ok | {:error, :no_compaction_running}

  @doc """
  Stops a running compaction.

  If a compaction operation is running, it is halted, and the function returns
  `:ok`. Otherwise it returns `{:error, :no_compaction_running}`. If a new
  compaction is started (manually or automatically), it will start from scratch,
  the halted compaction is completely discarded.

  This function can be useful if one wants to make sure that no compaction
  operation is running in a certain moment, for example to perform some
  write-intensive workload without incurring in additional load. In this case
  one can pause auto compaction, and call `halt_compaction/1` to stop any
  running compaction.
  """
  def halt_compaction(db) do
    GenServer.call(db, :halt_compaction, :infinity)
  end

  @spec compacting?(server) :: boolean

  @doc """
  Returns true if a compaction operation is currently running, false otherwise.
  """
  def compacting?(db) do
    GenServer.call(db, :compacting?, :infinity)
  end

  @spec file_sync(server) :: :ok

  @doc """
  Performs a `fsync`, forcing to flush all data that might be buffered by the OS
  to disk.

  Calling this function ensures that all writes up to this point are committed
  to disk, and will be available after a restart.

  If `CubDB` is started with the option `auto_file_sync: true`, calling this
  function is not necessary, as every write operation will be automatically
  flushed to the storage device.

  If this function is NOT called, the operative system will control when the
  file buffer is flushed to the storage device, which leads to better write
  performance, but might affect durability of recent writes in case of a sudden
  shutdown.
  """

  def file_sync(db) do
    GenServer.call(db, :file_sync, :infinity)
  end

  @spec set_auto_file_sync(server, boolean) :: :ok

  @doc """
  Configures whether to automatically force file sync upon each write operation.

  If set to `false`, no automatic file sync is performed. That improves write
  performance, but leaves to the operative system the decision of when to flush
  disk buffers. This means that there is the possibility that recent writes
  might not be durable in case of a sudden machine shutdown. In any case,
  atomicity of multi operations is preserved, and partial writes will not
  corrupt the database.

  If set to `true`, the file buffer will be forced to flush upon every write
  operation, ensuring durability even in case of sudden machine shutdowns, but
  decreasing write performance.
  """
  def set_auto_file_sync(db, bool) do
    GenServer.call(db, {:set_auto_file_sync, bool}, :infinity)
  end

  @spec data_dir(server) :: String.t()

  @doc """
  Returns the path of the data directory, as given when the `CubDB` process was
  started.

  ## Example

      {:ok, db} = CubDB.start_link("some/data/directory")

      CubDB.data_dir(db)
      #=> "some/data/directory"
  """

  def data_dir(db) do
    GenServer.call(db, :data_dir, :infinity)
  end

  @spec current_db_file(server) :: String.t()

  @doc """
  Returns the path of the current database file.

  The current database file will change after a compaction operation.

  ## Example

      {:ok, db} = CubDB.start_link("some/data/directory")

      CubDB.current_db_file(db)
      #=> "some/data/directory/0.cub"
  """

  def current_db_file(db) do
    GenServer.call(db, :current_db_file, :infinity)
  end

  @spec back_up(server, Path.t()) :: :ok | {:error, term}

  @doc """
  Creates a backup of the database into the target directory path

  The directory is created upon calling `back_up/2`, and an error tuple is
  returned if it already exists.

  The function will block until the backup is completed, then return :ok. The
  backup does not block other readers or writers, and reflects the database
  state at the time it was started, without any later write.

  After the backup completes successfully, it is possible to open it by starting
  a `CubDB` process using the target path as its data directory.
  """
  def back_up(db, target_path) do
    with_snapshot(db, fn snapshot ->
      Snapshot.back_up(snapshot, target_path)
    end)
  end

  @spec cubdb_file?(String.t()) :: boolean

  @doc false
  def cubdb_file?(file_name) do
    extension = Path.extname(file_name)
    basename = Path.basename(file_name, extension)

    extension in @known_file_extensions && basename =~ ~r/^[\da-fA-F]+$/
  end

  @spec compaction_file?(String.t()) :: boolean

  @doc false
  def compaction_file?(file_name) do
    Path.extname(file_name) == @compaction_file_extension
  end

  @spec subscribe(server) :: :ok

  @doc false
  def subscribe(db) do
    GenServer.call(db, {:subscribe, self()}, :infinity)
  end

  @spec file_name_to_n(String.t()) :: integer

  @doc false
  def file_name_to_n(file_name) do
    base_name = Path.basename(file_name, Path.extname(file_name))
    String.to_integer(base_name, 16)
  end

  # OTP callbacks

  @impl true
  def init([data_dir, options, gen_server_options]) do
    auto_compact = parse_auto_compact!(Keyword.get(options, :auto_compact, true))
    auto_file_sync = Keyword.get(options, :auto_file_sync, true)
    gen_server_options = Keyword.take(gen_server_options, [:hibernate_after])

    with {:ok, file_name} <- find_db_file(data_dir),
         file_name = file_name || "0#{@db_file_extension}",
         {:ok, store} <- Store.File.create(Path.join(data_dir, file_name), gen_server_options),
         {:ok, clean_up} <- CleanUp.start_link(data_dir, gen_server_options),
         {:ok, task_supervisor} <- Task.Supervisor.start_link() do
      {:ok,
       %State{
         btree: Btree.new(store),
         task_supervisor: task_supervisor,
         data_dir: data_dir,
         clean_up: clean_up,
         auto_compact: auto_compact,
         auto_file_sync: auto_file_sync
       }}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, state) do
    do_halt_compaction(state)

    GenServer.stop(state.clean_up)

    Btree.stop(state.btree)

    for old_btree <- state.old_btrees do
      if Btree.alive?(old_btree), do: :ok = Btree.stop(old_btree)
    end
  end

  @impl true
  def handle_call({:snapshot, ttl}, _, state) do
    %State{btree: btree} = state

    ref = make_ref()
    state = checkin_reader(ref, btree, state)
    snapshot = %Snapshot{db: self(), btree: btree, reader_ref: ref}

    case ttl do
      :infinity ->
        {:reply, snapshot, state}

      ttl when is_integer(ttl) ->
        Process.send_after(self(), {:snapshot_timeout, ref}, ttl)
        {:reply, snapshot, state}
    end
  end

  def handle_call({:release_snapshot, snapshot}, _, state) do
    %Snapshot{reader_ref: ref} = snapshot
    {:reply, :ok, checkout_reader(ref, state)}
  end

  def handle_call({:extend_snapshot, snapshot}, _, state) do
    %Snapshot{reader_ref: ref, btree: btree} = snapshot
    %State{readers: readers} = state

    if Map.has_key?(readers, ref) do
      new_ref = make_ref()
      state = checkin_reader(new_ref, btree, state)
      snapshot = %Snapshot{db: self(), btree: btree, reader_ref: new_ref}

      {:reply, {:ok, snapshot}, state}
    else
      {:reply, {:error, :invalid}, state}
    end
  end

  def handle_call(:dirt_factor, _, state = %State{btree: btree}) do
    {:reply, Btree.dirt_factor(btree), state}
  end

  def handle_call(:start_transaction, from, state = %State{writer: nil}) do
    %State{btree: btree} = state

    tx = %Tx{
      btree: btree,
      compacting: compaction_running?(state),
      owner: from,
      db: self()
    }

    {:reply, tx, %State{state | writer: from}}
  end

  def handle_call(:start_transaction, {pid, _}, state = %State{writer: {pid, _}}) do
    {:reply, {:error, :already_in_transaction}, state}
  end

  def handle_call(:start_transaction, from, state) do
    %State{write_queue: queue} = state
    {:noreply, %State{state | write_queue: :queue.in(from, queue)}}
  end

  def handle_call(:cancel_transaction, {pid, _}, state = %State{writer: {pid, _}}) do
    {:reply, :ok, advance_write_queue(state)}
  end

  def handle_call({:commit_transaction, %Tx{owner: owner}}, _, state = %State{writer: writer})
      when owner != writer do
    {:reply, {:error, :invalid_owner}, state}
  end

  def handle_call({:commit_transaction, tx}, {pid, _}, state = %State{writer: {pid, _}}) do
    %Tx{btree: btree, recompact: recompact} = tx
    %State{btree: current_btree, old_btrees: old_btrees} = state

    btree = if btree != current_btree, do: maybe_sync(Btree.commit(btree), state), else: btree

    %Btree{store: current_store} = current_btree
    %Btree{store: new_store} = btree

    # If store changed, this write is completing a compaction
    state =
      if new_store != current_store do
        trigger_clean_up(%State{
          state
          | btree: finalize_compaction(btree),
            old_btrees: [current_btree | old_btrees]
        })
      else
        %State{state | btree: btree}
      end

    state = advance_write_queue(state)

    state =
      if recompact do
        state = do_halt_compaction(state)
        do_compact(state)
      else
        maybe_auto_compact(state)
      end

    {:reply, :ok, state}
  end

  def handle_call({:validate_transaction, %Tx{owner: owner}}, _, state = %State{writer: owner}) do
    {:reply, :ok, state}
  end

  def handle_call({:validate_transaction, _}, _, state) do
    {:reply, :error, state}
  end

  def handle_call(:compact, _, state) do
    case trigger_compaction(state) do
      {:ok, state} ->
        {:reply, :ok, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:set_auto_compact, setting}, _, state) do
    case parse_auto_compact(setting) do
      {:ok, setting} -> {:reply, :ok, %State{state | auto_compact: setting}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:set_auto_file_sync, bool}, _, state) do
    {:reply, :ok, %State{state | auto_file_sync: bool}}
  end

  def handle_call({:subscribe, pid}, _, state = %State{subs: subs}) do
    {:reply, :ok, %State{state | subs: [pid | subs]}}
  end

  def handle_call(:file_sync, _, state = %State{btree: btree}) do
    btree = Btree.sync(btree)
    {:reply, :ok, %State{state | btree: btree}}
  end

  def handle_call(:data_dir, _, state = %State{data_dir: data_dir}) do
    {:reply, data_dir, state}
  end

  def handle_call(:current_db_file, _, state = %State{btree: btree}) do
    %Btree{store: store} = btree
    %Store.File{file_path: file_path} = store
    {:reply, file_path, state}
  end

  def handle_call(:halt_compaction, _, state) do
    if compaction_running?(state) do
      state = state |> do_halt_compaction() |> trigger_clean_up()
      {:reply, :ok, state}
    else
      {:reply, {:error, :no_compaction_running}, state}
    end
  end

  def handle_call(:compacting?, _, state) do
    {:reply, compaction_running?(state), state}
  end

  @impl true
  def handle_info(message, state)
      when message == :compaction_completed or message == :catch_up_completed do
    for pid <- state.subs, do: send(pid, message)
    {:noreply, state}
  end

  def handle_info({:snapshot_timeout, ref}, state) do
    {:noreply, checkout_reader(ref, state)}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state = %State{compactor: pid}) do
    if state.compacting_store != nil && Store.open?(state.compacting_store) do
      Store.close(state.compacting_store)
    end

    {:noreply, %State{state | compactor: nil, compacting_store: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  @spec maybe_sync(Btree.t(), State.t()) :: Btree.t()

  defp maybe_sync(btree, %State{auto_file_sync: false}), do: btree
  defp maybe_sync(btree, %State{auto_file_sync: true}), do: Btree.sync(btree)

  @spec checkin_reader(reference, Btree.t(), State.t()) :: State.t()

  defp checkin_reader(ref, btree, state) do
    %State{readers: readers} = state
    %Btree{store: %Store.File{file_path: file_path}} = btree

    %State{state | readers: Map.put(readers, ref, file_path)}
  end

  @spec checkout_reader(reference, State.t()) :: State.t()

  defp checkout_reader(ref, state) do
    %State{readers: readers} = state

    case Map.pop(readers, ref) do
      {nil, _readers} ->
        state

      {_, readers} ->
        if state.clean_up_pending == true do
          trigger_clean_up(%State{state | readers: readers})
        else
          %State{state | readers: readers}
        end
    end
  end

  @spec find_db_file(String.t()) :: {:ok, String.t() | nil} | {:error, any}

  defp find_db_file(data_dir) do
    with :ok <- File.mkdir_p(data_dir),
         {:ok, files} <- File.ls(data_dir) do
      file =
        files
        |> Enum.filter(&(cubdb_file?(&1) && String.ends_with?(&1, @db_file_extension)))
        |> Enum.sort_by(&file_name_to_n/1)
        |> List.last()

      {:ok, file}
    end
  end

  @spec trigger_compaction(State.t()) :: {:ok, State.t()} | {:error, any}

  defp trigger_compaction(state = %State{data_dir: data_dir, clean_up: clean_up}) do
    case compaction_running?(state) do
      false ->
        for pid <- state.subs, do: send(pid, :compaction_started)
        {:ok, store} = new_compaction_store(data_dir)
        CleanUp.clean_up_old_compaction_files(clean_up, store)

        case Task.Supervisor.start_child(state.task_supervisor, Compactor, :run, [
               self(),
               store
             ]) do
          {:ok, pid} ->
            Process.monitor(pid)
            {:ok, %State{state | compactor: pid, compacting_store: store}}

          {:error, cause} ->
            Store.close(store)
            {:error, cause}
        end

      true ->
        {:error, :pending_compaction}
    end
  end

  @spec do_compact(State.t()) :: State.t()

  defp do_compact(state) do
    case trigger_compaction(state) do
      {:ok, state} ->
        state

      {:error, _} ->
        state
    end
  end

  @spec finalize_compaction(Btree.t()) :: Btree.t()

  defp finalize_compaction(btree = %Btree{store: compacted_store}) do
    Btree.sync(btree)
    Store.close(compacted_store)

    new_path =
      String.replace_suffix(
        compacted_store.file_path,
        @compaction_file_extension,
        @db_file_extension
      )

    :ok = File.rename(compacted_store.file_path, new_path)

    {:ok, store} = Store.File.create(new_path)
    Btree.new(store)
  end

  @spec new_compaction_store(String.t()) :: {:ok, Store.t()} | {:error, any}

  defp new_compaction_store(data_dir) do
    with {:ok, file_names} <- File.ls(data_dir) do
      new_filename =
        file_names
        |> Enum.filter(&cubdb_file?/1)
        |> Enum.map(&file_name_to_n/1)
        |> Enum.sort()
        |> List.last()
        |> (&(&1 + 1)).()
        |> Integer.to_string(16)
        |> (&(&1 <> @compaction_file_extension)).()

      Store.File.create(Path.join(data_dir, new_filename))
    end
  end

  @spec compaction_running?(State.t()) :: boolean

  defp compaction_running?(%State{compactor: nil}), do: false

  defp compaction_running?(_), do: true

  @spec do_halt_compaction(State.t()) :: State.t()

  defp do_halt_compaction(state = %State{compactor: nil}), do: state

  defp do_halt_compaction(state = %State{compactor: pid}) do
    if pid != nil do
      Process.exit(pid, :halt)
      Store.close(state.compacting_store)
    end

    %State{state | compactor: nil, compacting_store: nil}
  end

  @spec trigger_clean_up(State.t()) :: State.t()

  defp trigger_clean_up(state) do
    if can_clean_up?(state),
      do: clean_up_now(state),
      else: clean_up_when_possible(state)
  end

  @spec can_clean_up?(State.t()) :: boolean

  defp can_clean_up?(%State{btree: %Btree{store: store}, readers: readers}) do
    %Store.File{file_path: file_path} = store

    Enum.all?(readers, fn {_reader, file} ->
      file == file_path
    end)
  end

  @spec clean_up_now(State.t()) :: State.t()

  defp clean_up_now(state = %State{btree: btree, clean_up: clean_up}) do
    for old_btree <- state.old_btrees do
      if Btree.alive?(old_btree), do: :ok = Btree.stop(old_btree)
    end

    :ok = CleanUp.clean_up(clean_up, btree.store)
    for pid <- state.subs, do: send(pid, :clean_up_started)
    %State{state | clean_up_pending: false, old_btrees: []}
  end

  @spec clean_up_when_possible(State.t()) :: State.t()

  defp clean_up_when_possible(state) do
    %State{state | clean_up_pending: true}
  end

  @spec maybe_auto_compact(State.t()) :: State.t()

  defp maybe_auto_compact(state) do
    if should_auto_compact?(state) do
      do_compact(state)
    else
      state
    end
  end

  @spec should_auto_compact?(State.t()) :: boolean

  defp should_auto_compact?(%State{auto_compact: false}), do: false

  defp should_auto_compact?(%State{btree: btree, auto_compact: auto_compact}) do
    {min_writes, min_dirt_factor} = auto_compact
    %Btree{dirt: dirt} = btree
    dirt_factor = Btree.dirt_factor(btree)
    dirt >= min_writes and dirt_factor >= min_dirt_factor
  end

  @spec advance_write_queue(State.t()) :: State.t()

  defp advance_write_queue(state) do
    %State{write_queue: queue, btree: btree} = state

    {writer, queue} =
      case :queue.out(queue) do
        {{:value, next}, queue} ->
          GenServer.reply(next, %Tx{
            btree: btree,
            compacting: compaction_running?(state),
            owner: next,
            db: self()
          })

          {next, queue}

        {:empty, queue} ->
          {nil, queue}
      end

    %State{state | writer: writer, write_queue: queue}
  end

  @spec parse_auto_compact(any) :: {:ok, auto_compact} | {:error, any}

  defp parse_auto_compact(setting) do
    case setting do
      false ->
        {:ok, false}

      true ->
        {:ok, @auto_compact_defaults}

      {min_writes, min_dirt_factor} when is_integer(min_writes) and is_number(min_dirt_factor) ->
        if min_writes >= 0 and min_dirt_factor >= 0 and min_dirt_factor <= 1,
          do: {:ok, {min_writes, min_dirt_factor}},
          else: {:error, "invalid auto compact setting"}

      _ ->
        {:error, "invalid auto compact setting"}
    end
  end

  @spec parse_auto_compact!(any) :: auto_compact

  defp parse_auto_compact!(setting) do
    case parse_auto_compact(setting) do
      {:ok, setting} -> setting
      {:error, reason} -> raise(ArgumentError, message: reason)
    end
  end

  @spec split_options(
          [option | data_dir_option | GenServer.option()]
          | String.t()
        ) :: {:ok, {String.t(), [option], GenServer.options()}} | {:error, term}

  defp split_options(data_dir) when is_binary(data_dir) do
    {:ok, {data_dir, [], []}}
  end

  defp split_options(data_dir_or_options) do
    case Keyword.pop(data_dir_or_options, :data_dir) do
      {nil, data_dir_or_options} ->
        try do
          {:ok, {to_string(data_dir_or_options), [], []}}
        rescue
          ArgumentError ->
            {:error, "Options must include :data_dir"}

          Protocol.UndefinedError ->
            {:error, "data_dir must be a string (or implement String.Chars)"}
        end

      {data_dir, options} ->
        {gen_server_opts, opts} =
          Keyword.split(options, [:name, :timeout, :spawn_opt, :hibernate_after, :debug])

        try do
          {:ok, {to_string(data_dir), opts, gen_server_opts}}
        rescue
          Protocol.UndefinedError ->
            {:error, "data_dir must be a string (or implement String.Chars)"}
        end
    end
  end
end
