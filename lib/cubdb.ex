defmodule CubDB do
  @moduledoc """
  `CubDB` is an embedded key-value database written in the Elixir language. It
  runs locally, it is schema-less, and backed by a single file.

  ## Fetaures

    - Both keys and values can be any arbitrary Elixir (or Erlang) term.

    - Simple `get/3`, `put/3`, and `delete/2` operations

    - Arbitrary selection of ranges of entries sorted by key with `select/2`

    - Atomic transactions with `put_multi/2`, `get_and_update_multi/4`, etc.

    - Concurrent read operations, that do not block nor are blocked by writes

    - Unexpected shutdowns won't corrupt the database or break atomicity

    - Manual or automatic compaction to optimize space usage

  To ensure consistency, performance, and robustness to data corruption, `CubDB`
  database file uses an append-only, immutable B-tree data structure. Entries
  are never changed in-place, and read operations are performend on immutable
  snapshots.

  More information can be found in the following sections:

    - [Frequently Asked Questions](faq.html)
    - [How To](howto.html)

  ## Usage

  Start `CubDB` by specifying a directory for its database file (if not existing,
  it will be created):

      {:ok, db} = CubDB.start_link("my/data/directory")

  Alternatively, to specify more options, a keyword list can be passed:

      {:ok, db} = CubDB.start_link(data_dir: "my/data/directory", auto_compact: true)

  _Important: avoid starting multiple `CubDB` processes on the same data
  directory. Only one `CubDB` process should use a specific data directory at any
  time._

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

  Multiple operations can be performed as an atomic transaction with
  `put_multi/2`, `delete_multi/2`, and the other `[...]_multi` functions:

      CubDB.put_multi(db, [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8])
      #=> :ok

  Range of entries sorted by key are retrieved using `select/2`:

      CubDB.select(db, min_key: :b, max_key: :e)
      #=> {:ok, [b: 2, c: 3, d: 4, e: 5]}

  But `select/2` can do much more than that. It can apply a pipeline of operations
  (`map`, `filter`, `take`, `drop` and more) to the selected entries, it can
  select the entries in normal or reverse order, and it can `reduce` the result
  using an arbitrary function:

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

  Because `CubDB` uses an immutable data structure, write operations cause the
  data file to grow. When necessary, `CubDB` runs a compaction operation to
  optimize the file size and reclaim disk space. Compaction runs in the
  background, without blocking other operations. By default, `CubDB` runs
  compaction automatically when necessary (see documentation of
  `set_auto_compact/2` for details). Alternatively, it can be started manually
  by calling `compact/1`.
  """

  @doc """
  Returns a specification to start this module under a supervisor.

  The default options listed in `Supervisor` are used.
  """
  use GenServer

  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Reader
  alias CubDB.Compactor
  alias CubDB.CatchUp
  alias CubDB.CleanUp

  @db_file_extension ".cub"
  @compaction_file_extension ".compact"
  @auto_compact_defaults {100, 0.25}

  @type key :: any
  @type value :: any
  @type entry :: {key, value}
  @type option :: {:auto_compact, {pos_integer, number} | boolean} | {:auto_file_sync, boolean}
  @type pipe_operation ::
          {:map, fun}
          | {:filter, fun}
          | {:take, non_neg_integer}
          | {:drop, non_neg_integer}
          | {:take_while, fun}
          | {:drop_while, fun}
  @type select_option ::
          {:min_key, any}
          | {:max_key, any}
          | {:min_key_inclusive, boolean}
          | {:max_key_inclusive, boolean}
          | {:pipe, [pipe_operation]}
          | {:reverse, boolean}
          | {:reduce, fun | {any, fun}}
          | {:timeout, timeout}

  defmodule State do
    @moduledoc false

    @type t :: %CubDB.State{
            btree: Btree.t(),
            data_dir: String.t(),
            task_supervisor: pid,
            compactor: pid | nil,
            catch_up: pid | nil,
            clean_up: pid,
            clean_up_pending: boolean,
            readers: %{required(reference) => {String.t(), reference}},
            auto_compact: {pos_integer, number} | false,
            auto_file_sync: boolean,
            subs: list(pid)
          }

    @enforce_keys [:btree, :data_dir, :clean_up]
    defstruct [
      :task_supervisor,
      btree: nil,
      data_dir: nil,
      compactor: nil,
      catch_up: nil,
      clean_up: nil,
      clean_up_pending: false,
      readers: %{},
      auto_compact: true,
      auto_file_sync: true,
      subs: []
    ]
  end

  @spec start_link(
          String.t()
          | [option | {:data_dir, String.t()} | GenServer.option()]
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
        GenServer.start_link(__MODULE__, [data_dir, options], gen_server_options)

      error ->
        error
    end
  end

  def start_link(data_dir, options) do
    start_link(Keyword.merge(options, data_dir: data_dir))
  end

  @spec start(String.t() | [option | {:data_dir, String.t()} | GenServer.option()]) ::
          GenServer.on_start()

  @doc """
  Starts the `CubDB` database without a link.

  See `start_link/2` for more information about options.
  """
  def start(data_dir_or_options) do
    case split_options(data_dir_or_options) do
      {:ok, {data_dir, options, gen_server_options}} ->
        GenServer.start(__MODULE__, [data_dir, options], gen_server_options)

      error ->
        error
    end
  end

  def start(data_dir, options) do
    start(Keyword.merge(options, data_dir: data_dir))
  end

  @spec stop(GenServer.server(), term(), timeout()) :: :ok

  @doc """
  Synchronously stops the `CubDB` database.

  See `GenServer.stop/3` for details.
  """

  def stop(db, reason \\ :normal, timeout \\ :infinity) do
    GenServer.stop(db, reason, timeout)
  end

  @spec get(GenServer.server(), key, value) :: value

  @doc """
  Gets the value associated to `key` from the database.

  If no value is associated with `key`, `default` is returned (which is `nil`,
  unless specified otherwise).
  """
  def get(db, key, default \\ nil) do
    perform_read(db, {:get, key, default})
  end

  @spec fetch(GenServer.server(), key) :: {:ok, value} | :error

  @doc """
  Fetches the value for the given `key` in the database, or return `:error` if `key` is not present.

  If the database contains an entry with the given `key` and value `value`, it
  returns `{:ok, value}`. If `key` is not found, it returns `:error`.
  """
  def fetch(db, key) do
    perform_read(db, {:fetch, key})
  end

  @spec has_key?(GenServer.server(), key) :: boolean

  @doc """
  Returns whether an entry with the given `key` exists in the database.
  """
  def has_key?(db, key) do
    perform_read(db, {:has_key?, key})
  end

  @spec select(GenServer.server(), [select_option]) ::
          {:ok, any} | {:error, Exception.t()}

  @doc """
  Selects a range of entries from the database, and optionally performs a
  pipeline of operations on them.

  It returns `{:ok, result}` if successful, or `{:error, exception}` if an
  exception is raised.

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

  As `nil` is a valid key, setting `min_key` or `max_key` to `nil` does NOT
  leave the range open ended:

      # Select entries where nil <= key <= "a"
      CubDB.select(db, min_key: nil, max_key: "a")

  The `reverse` option, when set to true, causes the entries to be selected and
  traversed in reverse order.

  The `pipe` option specifies an optional list of operations performed
  sequentially on the selected entries. The given order of operations is
  respected. The available operations, specified as tuples, are:

    - `{:filter, fun}` filters entries for which `fun` returns a truthy value

    - `{:map, fun}` maps each entry to the value returned by the function `fun`

    - `{:take, n}` takes the first `n` entries

    - `{:drop, n}` skips the first `n` entries

    - `{:take_while, fun}` takes entries while `fun` returns a truthy value

    - `{:drop_while, fun}` skips entries while `fun` returns a truthy value

  Note that, when selecting a key range, specifying `min_key` and/or `max_key`
  is more performant than using `{:filter, fun}` or `{:take_while | :drop_while,
  fun}`, because `min_key` and `max_key` avoid loading unnecessary entries from
  disk entirely.

  The `reduce` option specifies how the selected entries are aggregated. If
  `reduce` is omitted, the entries are returned as a list. If `reduce` is a
  function, it is used to reduce the collection of entries. If `reduce` is a
  tuple, the first element is the starting value of the reduction, and the
  second is the reducing function.

  The `timeout` option specifies a timeout (in milliseconds or `:infinity`,
  defaulting to `:infinity`) after which the operation will fail.

  ## Examples

  To select all entries with keys between `:a` and `:c` as a list of `{key,
  value}` entries we can do:

      {:ok, entries} = CubDB.select(db, min_key: :a, max_key: :c)

  If we want to get all entries with keys between `:a` and `:c`, with `:c`
  excluded, we can do:

      {:ok, entries} = CubDB.select(db,
        min_key: :a, max_key: :c, max_key_inclusive: false)

  To select the last 3 entries, we can do:

      {:ok, entries} = CubDB.select(db, reverse: true, pipe: [take: 3])

  If we want to obtain the sum of the first 10 positive numeric values
  associated to keys from `:a` to `:f`, we can do:

      {:ok, sum} = CubDB.select(db,
        min_key: :a,
        max_key: :f,
        pipe: [
          map: fn {_key, value} -> value end, # map values
          filter: fn n -> is_number(n) and n > 0 end # only positive numbers
          take: 10, # take only the first 10 entries in the range
        ],
        reduce: fn n, sum -> sum + n end # reduce to the sum of selected values
      )
  """
  def select(db, options \\ []) when is_list(options) do
    timeout = Keyword.get(options, :timeout, :infinity)
    perform_read(db, {:select, options}, timeout)
  end

  @spec size(GenServer.server()) :: pos_integer

  @doc """
  Returns the number of entries present in the database.
  """
  def size(db) do
    GenServer.call(db, :size, :infinity)
  end

  @spec dirt_factor(GenServer.server()) :: float

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

  @spec put(GenServer.server(), key, value) :: :ok

  @doc """
  Writes an entry in the database, associating `key` to `value`.

  If `key` was already present, it is overwritten.
  """
  def put(db, key, value) do
    GenServer.call(db, {:put, key, value}, :infinity)
  end

  @spec put_new(GenServer.server(), key, value) :: :ok | {:error, :exists}

  @doc """
  Writes an entry in the database, associating `key` to `value`, only if `key`
  is not yet in the database.

  If `key` is already present, it does not change it, and returns `{:error,
  :exists}`.
  """
  def put_new(db, key, value) do
    GenServer.call(db, {:put_new, key, value}, :infinity)
  end

  @spec delete(GenServer.server(), key) :: :ok

  @doc """
  Deletes the entry associated to `key` from the database.

  If `key` was not present in the database, nothing is done.
  """
  def delete(db, key) do
    GenServer.call(db, {:delete, key}, :infinity)
  end

  @spec update(GenServer.server(), key, value, (value -> value)) :: :ok

  @doc """
  Updates the entry corresponding to `key` using the given function.

  If `key` is present in the database, `fun` is invoked with the corresponding
  `value`, and the result is set as the new value of `key`. If `key` is not
  found, `initial` is inserted as the value of `key`.

  The return value is `:ok`, or `{:error, reason}` in case an error occurs.
  """
  def update(db, key, initial, fun) do
    with {:ok, nil} <-
           get_and_update_multi(db, [key], fn entries ->
             case Map.fetch(entries, key) do
               :error ->
                 {nil, %{key => initial}, []}

               {:ok, value} ->
                 {nil, %{key => fun.(value)}, []}
             end
           end),
         do: :ok
  end

  @spec get_and_update(GenServer.server(), key, (value -> {any, value} | :pop)) :: {:ok, any}

  @doc """
  Gets the value corresponding to `key` and updates it, in one atomic transaction.

  `fun` is called with the current value associated to `key` (or `nil` if not
  present), and must return a two element tuple: the result value to be
  returned, and the new value to be associated to `key`. `fun` may also return
  `:pop`, in which case the current value is deleted and returned.

  The return value is `{:ok, result}`, or `{:error, reason}` in case an error occurs.

  Note that in case the value to update returned by `fun` is the same as the
  original value, no write is performed to disk.
  """
  def get_and_update(db, key, fun) do
    with {:ok, result} <-
           get_and_update_multi(db, [key], fn entries ->
             value = Map.get(entries, key, nil)

             case fun.(value) do
               {result, ^value} -> {result, %{}, []}
               {result, new_value} -> {result, %{key => new_value}, []}
               :pop -> {value, %{}, [key]}
             end
           end),
         do: {:ok, result}
  end

  @spec get_and_update_multi(
          GenServer.server(),
          [key],
          (%{optional(key) => value} -> {any, %{optional(key) => value} | nil, [key] | nil}),
          [opt]
        ) :: {:ok, any} | {:error, any}
        when opt: {:timeout, timeout}

  @doc """
  Gets and updates or deletes multiple entries in an atomic transaction.

  Gets all values associated with keys in `keys_to_get`, and passes them as a
  map of `%{key => value}` entries to `fun`. If a key is not found, it won't be
  added to the map passed to `fun`. Updates the database and returns a result
  according to the return value of `fun`. Returns {`:ok`, return_value} in case
  of success, `{:error, reason}` otherwise.

  The function `fun` should return a tuple of three elements: `{return_value,
  entries_to_put, keys_to_delete}`, where `return_value` is an arbitrary value
  to be returned, `entries_to_put` is a map of `%{key => value}` entries to be
  written to the database, and `keys_to_delete` is a list of keys to be deleted.

  The read and write operations are executed as an atomic transaction, so they
  will either all succeed, or all fail. Note that `get_and_update_multi/4`
  blocks other write operations until it completes.

  The `options` argument is an optional keyword list of options, including:

    - `:timeout` - a timeout (in milliseconds or `:infinite`, defaulting to
    `5000`) for the operation, after which the function returns `{:error,
    :timeout}`. This is useful to avoid blocking other write operations for too
    long.

  ## Example

  Assuming a database of names as keys, and integer monetary balances as values,
  and we want to transfer 10 units from `"Anna"` to `"Joy"`, returning their
  updated balance:

      {:ok, {anna, joy}} = CubDB.get_and_update_multi(db, ["Anna", "Joy"], fn entries ->
        anna = Map.get(entries, "Anna", 0)
        joy = Map.get(entries, "Joy", 0)

        if anna < 10, do: raise(RuntimeError, message: "Anna's balance is too low")

        anna = anna - 10
        joy = joy + 10

        {{anna, joy}, %{"Anna" => anna, "Joy" => joy}, []}
      end)

  Or, if we want to transfer all of the balance from `"Anna"` to `"Joy"`,
  deleting `"Anna"`'s entry, and returning `"Joy"`'s resulting balance:

      {:ok, joy} = CubDB.get_and_update_multi(db, ["Anna", "Joy"], fn entries ->
        anna = Map.get(entries, "Anna", 0)
        joy = Map.get(entries, "Joy", 0)

        joy = joy + anna

        {joy, %{"Joy" => joy}, ["Anna"]}
      end)
  """
  def get_and_update_multi(db, keys_to_get, fun, options \\ []) do
    GenServer.call(db, {:get_and_update_multi, keys_to_get, fun, options}, :infinity)
  end

  @spec put_and_delete_multi(GenServer.server(), %{key => value}, [key]) :: :ok

  @doc """
  Writes and deletes multiple entries all at once, atomically.

  Entries to put are passed as a map of `%{key => value}` or a list of `{key,
  value}`. Keys to delete are passed as a list of keys.
  """
  def put_and_delete_multi(db, entries_to_put, keys_to_delete) do
    GenServer.call(db, {:put_and_delete_multi, entries_to_put, keys_to_delete})
  end

  @spec get_multi(GenServer.server(), [key]) :: %{key => value}

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
    perform_read(db, {:get_multi, keys})
  end

  @spec put_multi(GenServer.server(), %{key => value} | [entry]) :: :ok

  @doc """
  Writes multiple entries all at once, atomically.

  Entries are passed as a map of `%{key => value}` or a list of `{key, value}`.
  """
  def put_multi(db, entries) do
    put_and_delete_multi(db, entries, [])
  end

  @spec delete_multi(GenServer.server(), [key]) :: :ok

  @doc """
  Deletes multiple entries corresponding to the given keys all at once, atomically.

  The `keys` to be deleted are passed as a list.
  """
  def delete_multi(db, keys) do
    put_and_delete_multi(db, %{}, keys)
  end

  @spec compact(GenServer.server()) :: :ok | {:error, String.t()}

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

  @spec set_auto_compact(GenServer.server(), boolean | {integer, integer | float}) ::
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
  an external source, it is adviseable to turn off auto compaction, and manually
  run compaction at the end of the import.
  """
  def set_auto_compact(db, setting) do
    GenServer.call(db, {:set_auto_compact, setting}, :infinity)
  end

  @spec file_sync(GenServer.server()) :: :ok

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

  @spec set_auto_file_sync(GenServer.server(), boolean) :: :ok

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

  @spec data_dir(GenServer.server()) :: String.t()

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

  @spec current_db_file(GenServer.server()) :: String.t()

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

  @spec cubdb_file?(String.t()) :: boolean

  @doc false
  def cubdb_file?(file_name) do
    file_extensions = [@db_file_extension, @compaction_file_extension]
    basename = Path.basename(file_name, Path.extname(file_name))

    Enum.member?(file_extensions, Path.extname(file_name)) &&
      Regex.match?(~r/[\da-fA-F]+/, basename)
  end

  @spec compaction_file?(String.t()) :: boolean

  @doc false
  def compaction_file?(file_name) do
    Path.extname(file_name) == @compaction_file_extension
  end

  @doc false
  def subscribe(db) do
    GenServer.call(db, {:subscribe, self()}, :infinity)
  end

  @doc false
  def file_name_to_n(file_name) do
    base_name = Path.basename(file_name, Path.extname(file_name))
    String.to_integer(base_name, 16)
  end

  # OTP callbacks

  @doc false
  def init([data_dir, options]) do
    auto_compact = parse_auto_compact!(Keyword.get(options, :auto_compact, true))
    auto_file_sync = Keyword.get(options, :auto_file_sync, true)

    with file_name when is_binary(file_name) or is_nil(file_name) <- find_db_file(data_dir),
         {:ok, store} <-
           Store.File.create(Path.join(data_dir, file_name || "0#{@db_file_extension}")),
         {:ok, clean_up} <- CleanUp.start_link(data_dir),
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

  @doc false
  def terminate(_reason, %State{btree: btree}) do
    Btree.stop(btree)
  end

  def handle_call({:read, operation, timeout}, from, state) do
    %State{btree: btree, readers: readers} = state

    {:ok, pid} = Task.start_link(Reader, :run, [btree, from, operation])
    ref = Process.monitor(pid)

    timer =
      if timeout != :infinity do
        Process.send_after(self(), {:reader_timeout, pid}, timeout)
      else
        nil
      end

    %Btree{store: %Store.File{file_path: file_path}} = btree
    {:noreply, %State{state | readers: Map.put(readers, ref, {file_path, timer})}}
  end

  def handle_call(:size, _, state = %State{btree: btree}) do
    {:reply, Enum.count(btree), state}
  end

  def handle_call(:dirt_factor, _, state = %State{btree: btree}) do
    {:reply, Btree.dirt_factor(btree), state}
  end

  def handle_call({:put, key, value}, _, state) do
    %State{btree: btree, auto_file_sync: auto_file_sync} = state
    btree = Btree.insert(btree, key, value) |> Btree.commit()
    btree = if auto_file_sync, do: Btree.sync(btree), else: btree
    {:reply, :ok, maybe_auto_compact(%State{state | btree: btree})}
  end

  def handle_call({:put_new, key, value}, _, state) do
    %State{btree: btree, auto_file_sync: auto_file_sync} = state

    case Btree.insert_new(btree, key, value) do
      {:error, :exists} = reply ->
        {:reply, reply, state}

      btree ->
        btree = Btree.commit(btree)
        btree = if auto_file_sync, do: Btree.sync(btree), else: btree
        {:reply, :ok, maybe_auto_compact(%State{state | btree: btree})}
    end
  end

  def handle_call({:delete, key}, _, state) do
    %State{btree: btree, auto_file_sync: auto_file_sync} = state

    btree =
      case compaction_running?(state) do
        false -> Btree.delete(btree, key) |> Btree.commit()
        true -> Btree.mark_deleted(btree, key) |> Btree.commit()
      end

    btree = if auto_file_sync, do: Btree.sync(btree), else: btree

    {:reply, :ok, maybe_auto_compact(%State{state | btree: btree})}
  end

  def handle_call({:get_and_update_multi, keys_to_get, fun, options}, _, state) do
    %State{btree: btree} = state
    timeout = Keyword.get(options, :timeout, 5000)

    compute_update = fn ->
      key_values = Reader.perform(btree, {:get_multi, keys_to_get})
      fun.(key_values)
    end

    with {:ok, {result, entries_to_put, keys_to_delete}} <-
           run_with_timeout(compute_update, timeout) do
      state = do_put_and_delete_multi(state, entries_to_put, keys_to_delete)
      {:reply, {:ok, result}, state}
    else
      {:error, cause} ->
        {:reply, {:error, cause}, state}
    end
  end

  def handle_call({:put_and_delete_multi, entries_to_put, keys_to_delete}, _, state) do
    state = do_put_and_delete_multi(state, entries_to_put, keys_to_delete)
    {:reply, :ok, state}
  end

  def handle_call(:compact, _, state) do
    case trigger_compaction(state) do
      {:ok, compactor} ->
        {:reply, :ok, %State{state | compactor: compactor}}

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

  def handle_info({:compaction_completed, original_btree, compacted_btree}, state) do
    for pid <- state.subs, do: send(pid, :compaction_completed)
    {:noreply, catch_up(compacted_btree, original_btree, state)}
  end

  def handle_info({:catch_up, compacted_btree, original_btree}, state) do
    {:noreply, catch_up(compacted_btree, original_btree, state)}
  end

  def handle_info({:reader_timeout, reader}, state) do
    Process.unlink(reader)
    Process.exit(reader, :timeout)
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state = %State{compactor: pid}) do
    {:noreply, %State{state | compactor: nil}}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state = %State{catch_up: pid}) do
    {:noreply, %State{state | catch_up: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state = %State{readers: readers}) do
    # Process _might_ be a reader, so we remove it from the readers
    case Map.pop(readers, ref) do
      {nil, _readers} ->
        {:noreply, state}

      {{_, timer}, readers} ->
        if timer != nil, do: Process.cancel_timer(timer, async: true, info: false)

        if state.clean_up_pending == true do
          {:noreply, trigger_clean_up(%State{state | readers: readers})}
        else
          {:noreply, %State{state | readers: readers}}
        end
    end
  end

  @spec perform_read(GenServer.server(), Reader.operation(), timeout) :: any

  defp perform_read(db, operation, timeout \\ 5000) do
    GenServer.call(db, {:read, operation, timeout}, timeout)
  end

  @spec do_put_and_delete_multi(State.t(), [entry], [key]) :: State.t()

  defp do_put_and_delete_multi(state, entries_to_put, keys_to_delete) do
    %State{btree: btree, auto_file_sync: auto_file_sync} = state

    btree =
      Enum.reduce(entries_to_put || [], btree, fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    btree =
      Enum.reduce(keys_to_delete || [], btree, fn key, btree ->
        case compaction_running?(state) do
          false -> Btree.delete(btree, key)
          true -> Btree.mark_deleted(btree, key)
        end
      end)

    btree = Btree.commit(btree)

    btree = if auto_file_sync, do: Btree.sync(btree), else: btree

    maybe_auto_compact(%State{state | btree: btree})
  end

  @spec run_with_timeout(fun, timeout) :: {:ok, any} | {:error, any}

  defp run_with_timeout(fun, timeout) do
    task = Task.async(fun)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      nil ->
        {:error, :timeout}

      {:exit, reason} ->
        {:error, reason}

      {:ok, result} ->
        {:ok, result}
    end
  end

  @spec find_db_file(String.t()) :: String.t() | nil | {:error, any}

  defp find_db_file(data_dir) do
    with :ok <- File.mkdir_p(data_dir),
         {:ok, files} <- File.ls(data_dir) do
      files
      |> Enum.filter(&cubdb_file?/1)
      |> Enum.filter(&String.ends_with?(&1, @db_file_extension))
      |> Enum.sort_by(&file_name_to_n/1)
      |> List.last()
    end
  end

  @spec trigger_compaction(%State{}) :: {:ok, pid} | {:error, any}

  defp trigger_compaction(state = %State{btree: btree, data_dir: data_dir, clean_up: clean_up}) do
    case compaction_running?(state) do
      false ->
        for pid <- state.subs, do: send(pid, :compaction_started)
        {:ok, store} = new_compaction_store(data_dir)
        CleanUp.clean_up_old_compaction_files(clean_up, store)

        with result <-
               Task.Supervisor.start_child(state.task_supervisor, Compactor, :run, [
                 self(),
                 btree,
                 store
               ]),
             {:ok, pid} <- result do
          Process.monitor(pid)
          result
        end

      true ->
        {:error, :pending_compaction}
    end
  end

  @spec catch_up(Btree.t(), Btree.t(), State.t()) :: State.t()

  def catch_up(compacted_btree, original_btree, state) do
    %State{btree: latest_btree, task_supervisor: supervisor} = state

    if latest_btree == original_btree do
      compacted_btree = finalize_compaction(compacted_btree)
      state = %State{state | btree: compacted_btree}
      for pid <- state.subs, do: send(pid, :catch_up_completed)
      trigger_clean_up(state)
    else
      {:ok, pid} =
        Task.Supervisor.start_child(supervisor, CatchUp, :run, [
          self(),
          compacted_btree,
          original_btree,
          latest_btree
        ])

      Process.monitor(pid)
      %State{state | catch_up: pid}
    end
  end

  @spec finalize_compaction(Btree.t()) :: Btree.t()

  defp finalize_compaction(btree = %Btree{store: %Store.File{file_path: file_path}}) do
    Btree.sync(btree)

    new_path = String.replace_suffix(file_path, @compaction_file_extension, @db_file_extension)
    :ok = File.rename(file_path, new_path)

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

  @spec compaction_running?(%State{}) :: boolean

  defp compaction_running?(%State{compactor: nil, catch_up: nil}), do: false

  defp compaction_running?(_), do: true

  @spec trigger_clean_up(%State{}) :: %State{}

  defp trigger_clean_up(state) do
    if can_clean_up?(state),
      do: clean_up_now(state),
      else: clean_up_when_possible(state)
  end

  @spec can_clean_up?(%State{}) :: boolean

  defp can_clean_up?(%State{btree: %Btree{store: store}, readers: readers}) do
    %Store.File{file_path: file_path} = store

    Enum.all?(readers, fn {_reader, {file, _}} ->
      file == file_path
    end)
  end

  @spec clean_up_now(%State{}) :: %State{}

  defp clean_up_now(state = %State{btree: btree, clean_up: clean_up}) do
    :ok = CleanUp.clean_up(clean_up, btree)
    for pid <- state.subs, do: send(pid, :clean_up_started)
    %State{state | clean_up_pending: false}
  end

  @spec clean_up_when_possible(%State{}) :: %State{}

  defp clean_up_when_possible(state) do
    %State{state | clean_up_pending: true}
  end

  @spec maybe_auto_compact(%State{}) :: %State{}

  defp maybe_auto_compact(state) do
    if should_auto_compact?(state) do
      case trigger_compaction(state) do
        {:ok, compactor} ->
          %State{state | compactor: compactor}

        {:error, _} ->
          state
      end
    else
      state
    end
  end

  @spec should_auto_compact?(%State{}) :: boolean

  defp should_auto_compact?(%State{auto_compact: false}), do: false

  defp should_auto_compact?(%State{btree: btree, auto_compact: auto_compact}) do
    {min_writes, min_dirt_factor} = auto_compact
    %Btree{dirt: dirt} = btree
    dirt_factor = Btree.dirt_factor(btree)
    dirt >= min_writes and dirt_factor >= min_dirt_factor
  end

  @spec parse_auto_compact(any) :: {:ok, false | {pos_integer, number}} | {:error, any}

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

  @spec parse_auto_compact!(any) :: false | {pos_integer, number}

  defp parse_auto_compact!(setting) do
    case parse_auto_compact(setting) do
      {:ok, setting} -> setting
      {:error, reason} -> raise(ArgumentError, message: reason)
    end
  end

  @spec split_options(
          [option | {:data_dir, String.t()} | GenServer.option()]
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
