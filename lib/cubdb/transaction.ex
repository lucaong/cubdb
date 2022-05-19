defmodule CubDB.Tx do
  alias CubDB.Btree
  alias CubDB.Reader
  alias CubDB.Tx

  @enforce_keys [:btree, :compacting, :owner]
  defstruct [
    :btree,
    :compacting,
    :owner,
    recompact: false
  ]

  @typep transaction :: %Tx{
           btree: Btree.t(),
           compacting: boolean,
           owner: GenServer.from(),
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
  def get(%Tx{btree: btree}, key, default \\ nil) do
    Reader.perform(btree, {:get, key, default})
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
  def fetch(%Tx{btree: btree}, key) do
    Reader.perform(btree, {:fetch, key})
  end

  @spec has_key?(Tx.t(), CubDB.key()) :: boolean

  @doc """
  Returns whether an entry with the given `key` exists in the transaction.

  It works the same as `CubDB.has_key?/2`, but reads from a transaction instead
  of the live database.
  """
  def has_key?(%Tx{btree: btree}, key) do
    Reader.perform(btree, {:has_key?, key})
  end

  @spec select(Tx.t(), [CubDB.select_option()]) ::
          {:ok, any} | {:error, Exception.t()}

  @doc """
  Selects a range of entries from the transaction, and optionally performs a
  pipeline of operations on them.

  It works the same and accepts the same options as `CubDB.select/2`, but reads
  from a transaction instead of the live database.
  """
  def select(%Tx{btree: btree}, options \\ []) when is_list(options) do
    Reader.perform(btree, {:select, options})
  end

  @spec size(Tx.t()) :: non_neg_integer

  @doc """
  Returns the number of entries present in the transaction.

  It works the same as `CubDB.size/1`, but works on a transaction instead of the
  live database.
  """
  def size(%Tx{btree: btree}) do
    Enum.count(btree)
  end

  @spec put(Tx.t(), CubDB.key(), CubDB.value()) :: Tx.t()

  @doc """
  Writes an entry in the transaction, associating `key` to `value`.

  If `key` was already present, it is overwritten.

  Like other functions in this module, it does *not* mutate the transaction in
  place, and instead returns a modified transaction.
  """
  def put(tx = %Tx{btree: btree}, key, value) do
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
    case Btree.insert_new(btree, key, value) do
      {:error, :exists} = reply ->
        reply

      btree ->
        %Tx{tx | btree: btree}
    end
  end

  @spec delete(Tx.t(), CubDB.key()) :: Tx.t()

  @doc """
  Deletes the entry associated to `key` from the transaction.

  If `key` was not present, nothing is done.

  Like other functions in this module, it does *not* mutate the transaction in
  place, and instead returns a modified transaction.
  """
  def delete(tx = %Tx{btree: btree, compacting: compacting}, key) do
    if compacting do
      %Tx{tx | btree: Btree.mark_deleted(btree, key)}
    else
      %Tx{tx | btree: Btree.delete(btree, key)}
    end
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
    %Tx{tx | btree: Btree.clear(btree), recompact: compacting}
  end
end
