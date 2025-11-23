defmodule CubDB.Tx do
  @moduledoc """
  The `CubDB.Tx` module contains functions to read or write within atomic
  transactions created with `CubDB.transaction/2`.

  The functions in this module mirror the ones with the same name in the `CubDB`
  module, but work on transactions instead of on the live database.
  """

  alias CubDB.Btree
  alias CubDB.Reader
  alias CubDB.Snapshot
  alias CubDB.Tx

  @enforce_keys [:btree, :compacting, :owner, :db]
  defstruct [
    :btree,
    :compacting,
    :owner,
    :db,
    recompact: false
  ]

  @typep transaction :: %Tx{
           btree: Btree.t(),
           compacting: boolean,
           owner: GenServer.from(),
           db: GenServer.server(),
           recompact: boolean
         }

  @type t :: transaction

  @spec get(Tx.t(), CubDB.key(), any) :: CubDB.value()

  @doc """
  Gets the value associated to `key` from the transaction.

  If no value is associated with `key`, `default` is returned (which is `nil`,
  unless specified otherwise).

  It works the same as `CubDB.get/3`, but reads from a transaction instead of
  the live database.
  """
  def get(tx = %Tx{btree: btree}, key, default \\ nil) do
    validate_transaction!(tx)
    Reader.get(btree, key, default)
  end

  def get_metadata(tx = %Tx{btree: btree}, key, default \\ nil) do
    validate_transaction!(tx)
    Reader.get_metadata(btree, key, default)
  end

  @spec fetch(Tx.t(), CubDB.key()) :: {:ok, CubDB.value()} | :error

  @doc """
  Fetches the value for the given `key` from the transaction, or returns
  `:error` if `key` is not present.

  If the snapshot contains an entry with the given `key` and value `value`, it
  returns `{:ok, value}`. If `key` is not found, it returns `:error`.

  It works the same as `CubDB.fetch/2`, but reads from a transaction instead of
  the live database.
  """
  def fetch(tx = %Tx{btree: btree}, key) do
    validate_transaction!(tx)
    Reader.fetch(btree, key)
  end

  @spec has_key?(Tx.t(), CubDB.key()) :: boolean

  @doc """
  Returns whether an entry with the given `key` exists in the transaction.

  It works the same as `CubDB.has_key?/2`, but reads from a transaction instead
  of the live database.
  """
  def has_key?(tx = %Tx{btree: btree}, key) do
    validate_transaction!(tx)
    Reader.has_key?(btree, key)
  end

  @spec select(Tx.t(), [CubDB.select_option()]) :: Enumerable.t()

  @doc """
  Selects a range of entries from the transaction, returning a lazy stream.

  The lazy stream should be evaluated within the transaction scope, or a
  `RuntimeError` will be raised.

  It works the same and accepts the same options as `CubDB.select/2`, but reads
  from a transaction instead of the live database.
  """
  def select(%Tx{btree: btree} = tx, options \\ []) when is_list(options) do
    Stream.resource(
      fn ->
        validate_transaction!(tx)
        stream = Reader.select(btree, options)
        step = fn val, _acc -> {:suspend, val} end
        &Enumerable.reduce(stream, &1, step)
      end,
      fn next ->
        case next.({:cont, nil}) do
          {:done, _} ->
            {:halt, nil}

          {:suspended, value, next} ->
            {[value], next}
        end
      end,
      fn _ -> nil end
    )
  end

  @spec refetch(Tx.t(), CubDB.key(), Snapshot.t()) :: :unchanged | {:ok, CubDB.value()} | :error

  @doc """
  If `key` was not written since the point in time represented by `snapshot`,
  returns `:unchanged`. Otherwise, behaves like `fetch/2`.

  Checking if the key was written is done without fetching the whole entry, but
  instead only checking index pages. This makes `refetch/3` useful as a
  performance optimization in cases when one needs to verify if an entry changed
  with respect to a snapshot: using `refetch/3` can save some disk access if
  `CubDB` is able to determine that the key was not written since the snapshot.

  In some situations, such as when the entry is not present in the database, or
  when a compaction completed after `snapshot`, the function cannot determine if
  the entry was written or not since `snapshot`, and therefore fetches it. In
  other words, `refetch/3` is a performance optimization to save some disk reads
  when possible, but it might fetch an entry even if it technically did not
  change.

  ## Example

  The function `refetch/3` can be useful when implementing optimistic
  concurrency control. Suppose, for example, that computing the updated value of
  some entry is a slow operation. In order to avoid holding a transaction open
  for too long, one could compute the update outside of the transaction, and
  then check if the value on which the update was computed is still the same: if
  so, commit the update, otherwise perform the computation again. This kind of
  optimistic concurrency control can use `refetch/3` to avoid reading the value
  from disk when nothing has changed:

      def update_optimistically(db, key) do
        outcome = CubDB.with_snapshot(db, fn snap ->
          {:ok, value} = CubDB.Snapshot.fetch(snap, key)

          # Perform the slow calculation outside of the transaction
          new_value = some_slow_calculation(value)

          # In a transaction, check if the value changed, and update it if not
          write_if_unchanged(db, key, value, snap)
        end)

        # Depending on the outcome, return or recompute
        case outcome do
          :recompute ->
            update_optimistically(db, key)

          :ok ->
            :ok
        end
      end

      defp write_if_unchanged(db, key, value, snap) do
        CubDB.transaction(db, fn tx ->
          # Check if the value changed in the meanwhile
          case CubDB.Tx.refetch(tx, key, snap) do
            :unchanged ->
              # The entry was not written since we last read it. Commit the
              # new value and return :ok
              {:commit, CubDB.Tx.put(tx, key, new_value), :ok}

            {:ok, ^value} ->
              # The entry was written, but its value did not change. Commit the
              # new value and return :ok
              {:commit, CubDB.Tx.put(tx, key, new_value), :ok}

            _ ->
              # The entry changed since we last read it, cancel the
              # transaction and return :recompute
              {:cancel, :recompute}
          end
        end)
      end
  """
  def refetch(tx, key, snapshot)

  def refetch(%Tx{db: db}, _key, %Snapshot{db: other_db}) when db != other_db do
    raise "Invalid snapshot from another CubDB process"
  end

  def refetch(tx = %Tx{btree: btree}, key, %Snapshot{btree: reference_btree}) do
    validate_transaction!(tx)

    case Btree.written_since?(btree, key, reference_btree) do
      false ->
        :unchanged

      _ ->
        Reader.fetch(btree, key)
    end
  end

  @spec size(Tx.t()) :: non_neg_integer

  @doc """
  Returns the number of entries present in the transaction.

  It works the same as `CubDB.size/1`, but works on a transaction instead of the
  live database.
  """
  def size(tx = %Tx{btree: btree}) do
    validate_transaction!(tx)
    Reader.size(btree)
  end

  @spec put(Tx.t(), CubDB.key(), CubDB.value()) :: Tx.t()

  @doc """
  Writes an entry in the transaction, associating `key` to `value`.

  If `key` was already present, it is overwritten.

  Like other functions in this module, it does *not* mutate the transaction in
  place, and instead returns a modified transaction.
  """
  def put(tx = %Tx{btree: btree}, key, value) do
    validate_transaction!(tx)
    %Tx{tx | btree: Btree.insert(btree, key, value)}
  end

  @spec put_new(Tx.t(), CubDB.key(), CubDB.value()) :: Tx.t() | {:error, :exists}

  @doc """
  Writes an entry in the transaction, associating `key` to `value`, only if
  `key` is not yet present.

  If `key` is already present, it does not change it, and returns `{:error,
  :exists}`.

  Like other functions in this module, it does *not* mutate the transaction in
  place, and instead returns a modified transaction.
  """
  def put_new(tx = %Tx{btree: btree}, key, value) do
    validate_transaction!(tx)

    case Btree.insert_new(btree, key, value) do
      {:error, :exists} = reply ->
        reply

      btree ->
        %Tx{tx | btree: btree}
    end
  end

  def put_metadata(tx = %Tx{btree: btree}, key, value) do
    validate_transaction!(tx)
    %{tx | btree: Btree.insert_metadata(btree, key, value)}
  end

  @spec delete(Tx.t(), CubDB.key()) :: Tx.t()

  @doc """
  Deletes the entry associated to `key` from the transaction.

  If `key` was not present, nothing is done.

  Like other functions in this module, it does *not* mutate the transaction in
  place, and instead returns a modified transaction.
  """
  def delete(tx = %Tx{btree: btree, compacting: compacting}, key) do
    validate_transaction!(tx)

    if compacting do
      %Tx{tx | btree: Btree.mark_deleted(btree, key)}
    else
      %Tx{tx | btree: Btree.delete(btree, key)}
    end
  end

  def delete_metadata(tx = %Tx{btree: btree}, key) do
    validate_transaction!(tx)
    %{tx | btree: Btree.delete_metadata(btree, key)}
  end

  @spec clear(Tx.t()) :: Tx.t()

  @doc """
  Deletes all entries, resulting in an empty database.

  The deletion is much more performant than deleating each entry manually.

  It works like `CubDB.clear/1`, but on a transaction.

  If a compaction is in progress when `clear/1` is called, and the transaction
  is committed, the compaction is halted, and a new one started immediately
  after. The new compaction should be very fast, as the database is empty as a
  result of the `clear/1` call.

  Like other functions in this module, it does *not* mutate the transaction in
  place, and instead returns a modified transaction.
  """
  def clear(tx = %Tx{btree: btree, compacting: compacting}) do
    validate_transaction!(tx)
    %Tx{tx | btree: Btree.clear(btree), recompact: compacting}
  end

  defp validate_transaction!(tx = %Tx{db: db}) do
    case GenServer.call(db, {:validate_transaction, tx}, :infinity) do
      :ok ->
        :ok

      :error ->
        raise "Invalid transaction, likely because it was used outside of its scope"
    end
  end
end
