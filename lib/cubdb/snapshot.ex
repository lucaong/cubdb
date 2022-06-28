defmodule CubDB.Snapshot do
  @moduledoc """
  The `CubDB.Snapshot` module contains functions to read from snapshots obtained
  with `CubDB.with_snapshot/2` or `CubDB.snapshot/2`.

  The functions in this module mirror the ones with the same name in the `CubDB`
  module, but work on snapshots instead of on the live database.
  """

  alias CubDB.Btree
  alias CubDB.Reader
  alias CubDB.Snapshot
  alias CubDB.Store

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
  def get(snapshot = %Snapshot{}, key, default \\ nil) do
    with_extended_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.get(btree, key, default)
    end)
  end

  @spec get_multi(Snapshot.t(), [CubDB.key()]) :: %{CubDB.key() => CubDB.value()}

  @doc """
  Gets multiple entries corresponding by the given keys from the snapshot all at
  once, atomically.

  It works the same as `CubDB.get_multi/2`, but reads from a snapshot instead of
  the live database.
  """
  def get_multi(snapshot = %Snapshot{}, keys) do
    with_extended_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.get_multi(btree, keys)
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
  def fetch(snapshot = %Snapshot{}, key) do
    with_extended_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.fetch(btree, key)
    end)
  end

  @spec has_key?(Snapshot.t(), CubDB.key()) :: boolean

  @doc """
  Returns whether an entry with the given `key` exists in the snapshot.

  It works the same as `CubDB.has_key?/2`, but reads from a snapshot instead of
  the live database.
  """
  def has_key?(snapshot = %Snapshot{}, key) do
    with_extended_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.has_key?(btree, key)
    end)
  end

  @spec select(GenServer.server(), [CubDB.select_option()]) :: Enumerable.t()

  @doc """
  Selects a range of entries from the snapshot, returning a lazy stream.

  The lazy stream should be evaluated while the snapshot is still valid, or a
  `RuntimeError` will be raised.

  It works the same and accepts the same options as `CubDB.select/2`, but reads
  from a snapshot instead of the live database.
  """
  def select(snap, options \\ []) when is_list(options) do
    Stream.resource(
      fn ->
        snap = extend_snapshot(snap)
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

  @spec size(Snapshot.t()) :: non_neg_integer

  @doc """
  Returns the number of entries present in the snapshot.

  It works the same as `CubDB.size/1`, but works on a snapshot instead of the
  live database.
  """
  def size(snapshot = %Snapshot{}) do
    with_extended_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      Reader.size(btree)
    end)
  end

  @spec back_up(Snapshot.t(), Path.t()) :: :ok | {:error, term}

  @doc """
  Creates a backup of the snapshot into the target directory path

  It works the same as `CubDB.back_up/2`, but works on a snapshot instead of the
  live database.
  """
  def back_up(snapshot = %Snapshot{}, target_path) do
    with_extended_snapshot(snapshot, fn %Snapshot{btree: btree} ->
      with :ok <- File.mkdir(target_path),
           {:ok, store} <- Store.File.create(Path.join(target_path, "0#{@db_file_extension}")) do
        Btree.load(btree, store) |> Btree.sync()
        Store.close(store)
      end
    end)
  end

  @spec with_extended_snapshot(Snapshot.t(), (Snapshot.t() -> result)) :: result when result: any

  defp with_extended_snapshot(snapshot, fun) do
    snap = extend_snapshot(snapshot)

    try do
      fun.(snap)
    after
      CubDB.release_snapshot(snap)
    end
  end

  @spec extend_snapshot(Snapshot.t()) :: Snapshot.t()

  defp extend_snapshot(snapshot = %Snapshot{db: db}) do
    case GenServer.call(db, {:extend_snapshot, snapshot}, :infinity) do
      {:ok, snap} ->
        snap

      _ ->
        raise "Attempt to use CubDB snapshot after it was released or it timed out"
    end
  end
end
