defmodule CubDB.Snapshot do
  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Snapshot
  alias CubDB.Reader

  @db_file_extension ".cub"

  @enforce_keys [:db, :btree, :reader_ref]
  defstruct [
    :db,
    :btree,
    :reader_ref
  ]

  @typep snapshot :: %Snapshot{
           db: GenServer.server(),
           btree: Btree.t(),
           reader_ref: reference
         }

  @type t :: snapshot

  @spec get(Snapshot.t(), CubDB.key(), CubDB.value()) :: CubDB.value()

  @doc """
  Gets the value associated to `key` from the snapshot.

  If no value is associated with `key`, `default` is returned (which is `nil`,
  unless specified otherwise).

  It works the same as `CubDB.get/3`, but reads from a snapshot instead of the
  live database.
  """
  def get(%Snapshot{} = snapshot, key, default \\ nil) do
    extend_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.perform(btree, {:get, key, default})
    end)
  end

  @spec get_multi(Snapshot.t(), [CubDB.key()]) :: %{CubDB.key() => CubDB.value()}

  @doc """
  Gets multiple entries corresponding by the given keys from the snapshot all at
  once, atomically.

  It works the same as `CubDB.get_multi/2`, but reads from a snapshot instead of
  the live database.
  """
  def get_multi(%Snapshot{} = snapshot, keys) do
    extend_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.perform(btree, {:get_multi, keys})
    end)
  end

  @spec fetch(Snapshot.t(), CubDB.key()) :: {:ok, CubDB.value()} | :error

  @doc """
  Fetches the value for the given `key` from the snapshot, or returns `:error`
  if `key` is not present.

  If the snapshot contains an entry with the given `key` and value `value`, it
  returns `{:ok, value}`. If `key` is not found, it returns `:error`.

  It works the same as `CubDB.fetch/2`, but reads from a snapshot instead of
  the live database.
  """
  def fetch(%Snapshot{} = snapshot, key) do
    extend_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.perform(btree, {:fetch, key})
    end)
  end

  @spec has_key?(Snapshot.t(), CubDB.key()) :: boolean

  @doc """
  Returns whether an entry with the given `key` exists in the snapshot.

  It works the same as `CubDB.has_key?/2`, but reads from a snapshot instead of
  the live database.
  """
  def has_key?(%Snapshot{} = snapshot, key) do
    extend_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.perform(btree, {:has_key?, key})
    end)
  end

  @spec select(Snapshot.t(), [CubDB.select_option()]) ::
          {:ok, any} | {:error, Exception.t()}

  @doc """
  Selects a range of entries from the snapshot, and optionally performs a
  pipeline of operations on them.

  It returns `{:ok, result}` if successful, or `{:error, exception}` if an
  exception is raised.

  It works the same and accepts the same options as `CubDB.select/2`, but reads
  from a snapshot instead of the live database.
  """
  def select(%Snapshot{} = snapshot, options \\ []) when is_list(options) do
    extend_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.perform(btree, {:select, options})
    end)
  end

  @spec size(Snapshot.t()) :: non_neg_integer

  @doc """
  Returns the number of entries present in the snapshot.

  It works the same as `CubDB.size/1`, but works on a snapshot instead of the
  live database.
  """
  def size(%Snapshot{} = snapshot) do
    extend_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Enum.count(btree)
    end)
  end

  @spec back_up(Snapshot.t(), Path.t()) :: :ok | {:error, term}

  @doc """
  Creates a backup of the snapshot into the target directory path

  It works the same as `CubDB.back_up/2`, but works on a snapshot instead of the
  live database.
  """
  def back_up(%Snapshot{} = snapshot, target_path) do
    extend_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      with :ok <- File.mkdir(target_path),
           {:ok, store} <- Store.File.create(Path.join(target_path, "0#{@db_file_extension}")) do
        Btree.load(btree, store) |> Btree.sync()
        Store.close(store)
      end
    end)
  end

  @spec extend_snapshot(Snapshot.t(), (Snapshot.t() -> result)) :: result when result: any

  defp extend_snapshot(snapshot = %Snapshot{db: db}, fun) do
    case GenServer.call(db, {:extend_snapshot, snapshot}, :infinity) do
      {:ok, snap} ->
        try do
          fun.(snap)
        after
          CubDB.release_snapshot(snap)
        end

      _ ->
        raise "Attempt to use CubDB snapshot after it was released or it timed out"
    end
  end
end
