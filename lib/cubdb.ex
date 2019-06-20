defmodule CubDB do
  @moduledoc """
  `CubDB` is a pure-Elixir embedded key-value database, designed for simplicity.
  It runs locally, and is backed by a single file.

  Both keys and values can be any Elixir (or Erlang) term.

  The `CubDB` database file uses an immutable data structure that ensures
  robustness to data corruption: entries are never changed in-place, and writes
  are atomic.

  Read operations are performed on immutable views, so they are always
  consistent, run concurrently, and do not block write operations.

  ## Usage

  Start `CubDB` by specifying a directory for its database file (if not existing,
  it will be created):

      {:ok, db} = CubDB.start_link("my/data/directory")

  The `get/2`, `put/3`, and `delete/2` functions work as you probably expect:

      CubDB.put(db, :foo, "some value")
      #=> :ok

      CubDB.get(db, :foo)
      #=> "some value"

      CubDB.delete(db, :foo)
      #=> :ok

      CubDB.get(db, :foo)
      #=> nil

  Range of keys are retrieved using `select/3`:

      for {key, value} <- [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8] do
        CubDB.put(db, key, value)
      end

      CubDB.select(db, min_key: :b, max_key: :e)
      #=> {:ok, [b: 2, c: 3, d: 4, e: 5]}

  But `select/3` can do much more than that. It can apply a pipeline of operations
  (`map`, `filter`, `take`, `drop` and more) to the selected entries, it can
  select the entries in normal or reverse order, and it can `reduce` the result
  using an arbitrary function:

      # Take the sum of the last 3 even values:
      CubDB.select(db,
        reverse: true,
        pipe: [
          map: fn {_key, value} -> value end,
          filter: fn value -> Integer.is_even(value) end,
          take: 3
        ],
        reduce: fn n, sum -> sum + n end
      )
      #=> {:ok, 18}

  As `CubDB` uses an immutable data structure, write operations cause the data
  file to grow. Occasionally, it is adviseable to run a compaction to optimize
  the file size and reclaim disk space. Compaction is started manually by
  calling `compact/1`, and runs in the background, without blocking other
  operations:

      CubDB.compact(db)
      #=> :ok
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

  @type key :: any
  @type value :: any

  defmodule State do
    @moduledoc false

    @type t :: %CubDB.State{
            btree: Btree.t(),
            data_dir: binary,
            compactor: pid | nil,
            clean_up: pid,
            clean_up_pending: boolean,
            busy_files: %{required(binary) => pos_integer}
          }

    @enforce_keys [:btree, :data_dir, :clean_up]
    defstruct btree: nil,
              data_dir: nil,
              compactor: nil,
              clean_up: nil,
              clean_up_pending: false,
              busy_files: %{}
  end

  @spec start_link(binary, GenServer.options()) :: GenServer.on_start()

  @doc """
  Starts the `CubDB` database process linked to the current process.

  The `data_dir` argument is the directory path where the database files will be
  stored. If it does not exist, it will be created. Only one `CubDB` instance
  can run per directory, so if you run several databases, they should each use
  their own separate data directory.

  The `options` are passed to `GenServer.start_link/3`.
  """
  def start_link(data_dir, options \\ []) do
    GenServer.start_link(__MODULE__, data_dir, options)
  end

  @spec start(binary, GenServer.options()) :: GenServer.on_start()

  @doc """
  Starts the `CubDB` database without a link.

  See `start_link/2` for more informations.
  """
  def start(data_dir, options \\ []) do
    GenServer.start(__MODULE__, data_dir, options)
  end

  @spec get(GenServer.server(), key, value) :: value

  @doc """
  Gets the value associated to `key` from the database.

  If no value is associated with `key`, `default` is returned (which is `nil`,
  unless specified otherwise).
  """
  def get(db, key, default \\ nil) do
    GenServer.call(db, {:get, key, default})
  end

  @spec fetch(GenServer.server(), key) :: {:ok, value} | :error

  @doc """
  Fetches the value for the given `key` in the database, or return `:error` if `key` is not present.

  If the database contains an entry with the given `key` and value `value`, it
  returns `{:ok, value}`. If `key` is not found, it returns `:error`.
  """
  def fetch(db, key) do
    GenServer.call(db, {:fetch, key})
  end

  @spec has_key?(GenServer.server(), key) :: boolean

  @doc """
  Returns whether an entry with the given `key` exists in the database.
  """
  def has_key?(db, key) do
    GenServer.call(db, {:has_key?, key})
  end

  @spec select(GenServer.server(), Keyword.t(), timeout) ::
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

      # Select all entries where `"a" <= key <= "d"`
      CubDB.select(db, min_key: "b", max_key: "d")

  The range boundaries can be excluded by setting `min_key` or `max_key` to
  `{key, :excluded}`:

      # Select all entries where `"a" <= key < "d"`
      CubDB.select(db, min_key: "b", max_key: {"d", :excluded})

  Any of `:min_key` and `:max_key` can be omitted or set to `nil`, to leave the
  range open-ended.

      # Select entries where `key <= "a"
      CubDB.select(db, max_key: "a")

      # Or, equivalently:
      CubDB.select(db, min_key: nil, max_key: "a")

  In case the key boundary is the literal value `nil`, the longer form must be used:

      # Select entries where `nil <= key <= "a"`
      CubDB.select(db, min_key: {nil, :included}, max_key: "a")

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

  ## Examples

  To select all entries with keys between `:a` and `:c` as a list of `{key,
  value}` entries we can do:

      {:ok, entries} = CubDB.select(db, min_key: :a, max_key: :c)

  If we want to get all entries with keys between `:a` and `:c`, with `:c`
  exluded, we can do:

      {:ok, entries} = CubDB.select(db, min_key: :a, max_key: {:c, :excluded})

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
  def select(db, options \\ [], timeout \\ 5000) when is_list(options) do
    GenServer.call(db, {:select, options}, timeout)
  end

  @spec size(GenServer.server()) :: pos_integer

  @doc """
  Returns the number of entries present in the database.
  """
  def size(db) do
    GenServer.call(db, :size)
  end

  @spec dirt_factor(GenServer.server()) :: float

  @doc """
  Returns the dirt factor.

  The dirt factor is a number, ranging from 0 to 1, giving an indication about
  the amount of overhead storage (or "dirt") that can be cleaned up with a
  compaction operation. A value of 0 means that there is no overhead, so a
  compaction would have no benefit. The closer to 1 the dirt factor is, the more
  can be cleaned up in a compaction operation.
  """
  def dirt_factor(db) do
    GenServer.call(db, :dirt_factor)
  end

  @spec put(GenServer.server(), key, value) :: :ok

  @doc """
  Writes an entry in the database, associating `key` to `value`.

  If `key` was already present, it is overwritten.
  """
  def put(db, key, value) do
    GenServer.call(db, {:put, key, value})
  end

  @spec delete(GenServer.server(), key) :: :ok

  @doc """
  Deletes the entry associated to `key` from the database.

  If `key` was not present in the database, nothing is done.
  """
  def delete(db, key) do
    GenServer.call(db, {:delete, key})
  end

  @spec compact(GenServer.server()) :: :ok | {:error, binary}

  @doc """
  Runs a database compaction.

  As write operations are performed on a database, its file grows. Occasionally,
  a compaction operation can be run to shrink the file to its optimal size.
  Compaction runs in the background and does not block operations.

  Only one compaction operation can run at any time, therefore if this function
  is called when a compaction is already running, it returns `{:error,
  :pending_compaction}`.
  """
  def compact(db) do
    GenServer.call(db, :compact)
  end

  @spec cubdb_file?(binary) :: boolean

  @doc false
  def cubdb_file?(file_name) do
    file_extensions = [@db_file_extension, @compaction_file_extension]
    Enum.member?(file_extensions, Path.extname(file_name))
  end

  @spec db_file?(binary) :: boolean

  @doc false
  def db_file?(file_name) do
    Path.extname(file_name) == @db_file_extension
  end

  @spec compaction_file?(binary) :: boolean

  @doc false
  def compaction_file?(file_name) do
    Path.extname(file_name) == @compaction_file_extension
  end

  # OTP callbacks

  @doc false
  def init(data_dir) do
    case find_db_file(data_dir) do
      file_name when is_binary(file_name) or is_nil(file_name) ->
        store = Store.File.new(Path.join(data_dir, file_name || "0#{@db_file_extension}"))
        {:ok, clean_up} = CleanUp.start_link(data_dir)
        {:ok, %State{btree: Btree.new(store), data_dir: data_dir, clean_up: clean_up}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_call(operation = {:get, _, _}, from, state = %State{btree: btree}) do
    state = read(from, btree, operation, state)
    {:noreply, state}
  end

  def handle_call(operation = {:fetch, _}, from, state = %State{btree: btree}) do
    state = read(from, btree, operation, state)
    {:noreply, state}
  end

  def handle_call(operation = {:has_key?, _}, from, state = %State{btree: btree}) do
    state = read(from, btree, operation, state)
    {:noreply, state}
  end

  def handle_call(operation = {:select, _}, from, state = %State{btree: btree}) do
    state = read(from, btree, operation, state)
    {:noreply, state}
  end

  def handle_call(:size, _, state = %State{btree: btree}) do
    {:reply, Enum.count(btree), state}
  end

  def handle_call(:dirt_factor, _, state = %State{btree: btree}) do
    {:reply, Btree.dirt_factor(btree), state}
  end

  def handle_call({:put, key, value}, _, state = %State{btree: btree}) do
    btree = Btree.insert(btree, key, value)
    {:reply, :ok, %State{state | btree: btree}}
  end

  def handle_call({:delete, key}, _, state = %State{btree: btree, compactor: compactor}) do
    btree =
      case compactor do
        nil -> Btree.delete(btree, key)
        _ -> Btree.mark_deleted(btree, key)
      end

    {:reply, :ok, %State{state | btree: btree}}
  end

  def handle_call(:compact, _, state) do
    %State{btree: btree, data_dir: data_dir, clean_up: clean_up} = state

    reply =
      case can_compact?(state) do
        true ->
          {:ok, store} = new_compaction_store(data_dir)
          CleanUp.clean_up_old_compaction_files(clean_up, store)
          Compactor.start_link(self(), btree, store)

        {false, reason} ->
          {:error, reason}
      end

    case reply do
      {:ok, compactor} -> {:reply, :ok, %State{state | compactor: compactor}}
      error -> {:reply, error, state}
    end
  end

  def handle_info({:compaction_completed, original_btree, compacted_btree}, state) do
    send(self(), {:catch_up, compacted_btree, original_btree})
    {:noreply, state}
  end

  def handle_info({:catch_up, compacted_btree, original_btree}, state) do
    %State{btree: latest_btree} = state

    if latest_btree == original_btree do
      compacted_btree = finalize_compaction(compacted_btree)
      state = %State{state | btree: compacted_btree, compactor: nil}
      {:noreply, trigger_clean_up(state)}
    else
      CatchUp.start_link(self(), compacted_btree, original_btree, latest_btree)
      {:noreply, state}
    end
  end

  def handle_info(:clean_up_completed, state) do
    {:noreply, %State{state | clean_up: nil}}
  end

  def handle_info({:check_out_reader, btree}, state = %State{clean_up_pending: clean_up_pending}) do
    state = check_out_reader(btree, state)

    state =
      if clean_up_pending == true,
        do: trigger_clean_up(state),
        else: state

    {:noreply, state}
  end

  defp read(from, btree, operation, state) do
    Reader.start_link(from, self(), btree, operation)
    check_in_reader(btree, state)
  end

  defp find_db_file(data_dir) do
    with :ok <- File.mkdir_p(data_dir),
         {:ok, files} <- File.ls(data_dir) do
      files
      |> Enum.filter(&String.ends_with?(&1, @db_file_extension))
      |> Enum.sort()
      |> List.last()
    end
  end

  defp finalize_compaction(%Btree{store: %Store.File{file_path: file_path}}) do
    new_path = String.replace_suffix(file_path, @compaction_file_extension, @db_file_extension)
    :ok = File.rename(file_path, new_path)

    store = Store.File.new(new_path)
    Btree.new(store)
  end

  defp new_compaction_store(data_dir) do
    with {:ok, file_names} <- File.ls(data_dir) do
      new_filename =
        file_names
        |> Enum.filter(&cubdb_file?/1)
        |> Enum.map(fn file_name -> Path.basename(file_name, Path.extname(file_name)) end)
        |> Enum.sort()
        |> List.last()
        |> String.to_integer(16)
        |> (&(&1 + 1)).()
        |> Integer.to_string(16)
        |> (&(&1 <> @compaction_file_extension)).()

      store = Store.File.new(Path.join(data_dir, new_filename))
      {:ok, store}
    end
  end

  defp can_compact?(%State{compactor: compactor}) do
    case compactor do
      nil -> true
      _ -> {false, :pending_compaction}
    end
  end

  defp check_in_reader(%Btree{store: store}, state = %State{busy_files: busy_files}) do
    %Store.File{file_path: file_path} = store
    busy_files = Map.update(busy_files, file_path, 1, &(&1 + 1))
    %State{state | busy_files: busy_files}
  end

  defp check_out_reader(%Btree{store: store}, state = %State{busy_files: busy_files}) do
    %Store.File{file_path: file_path} = store

    busy_files =
      case Map.get(busy_files, file_path) do
        n when n > 1 -> Map.update!(busy_files, file_path, &(&1 - 1))
        _ -> Map.delete(busy_files, file_path)
      end

    %State{state | busy_files: busy_files}
  end

  defp trigger_clean_up(state) do
    if can_clean_up?(state),
      do: clean_up_now(state),
      else: clean_up_when_possible(state)
  end

  defp can_clean_up?(%State{btree: %Btree{store: store}, busy_files: busy_files}) do
    %Store.File{file_path: file_path} = store
    Enum.any?(busy_files, fn {file, _} -> file != file_path end) == false
  end

  defp clean_up_now(state = %State{btree: btree, clean_up: clean_up}) do
    :ok = CleanUp.clean_up(clean_up, btree)
    %State{state | clean_up_pending: false}
  end

  defp clean_up_when_possible(state) do
    %State{state | clean_up_pending: true}
  end
end
