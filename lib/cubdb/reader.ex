defmodule CubDB.Reader do
  @moduledoc false

  # The `CubDB.Reader` module performs all read operations that involve access
  # to the store. Read operations are performed on the Btree representing a
  # snapshot of the database at the time the read operation was invoked.

  alias CubDB.Btree

  @spec get(Btree.t(), CubDB.key(), any) :: any

  def get(btree, key, default) do
    case Btree.fetch(btree, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  @spec get_metadata(Btree.t(), CubDB.key(), any) :: any

  def get_metadata(btree, key, default) do
    case Btree.fetch_metadata(btree, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  @spec get_multi(Btree.t(), [CubDB.key()]) :: %{CubDB.key() => CubDB.value()}

  def get_multi(btree, keys) do
    Enum.reduce(keys, %{}, fn key, map ->
      case Btree.fetch(btree, key) do
        {:ok, value} -> Map.put(map, key, value)
        :error -> map
      end
    end)
  end

  @spec fetch(Btree.t(), CubDB.key()) :: {:ok, CubDB.value()} | :error

  def fetch(btree, key) do
    Btree.fetch(btree, key)
  end

  @spec has_key?(Btree.t(), CubDB.key()) :: boolean

  def has_key?(btree, key) do
    case Btree.fetch(btree, key) do
      {:ok, _} -> true
      :error -> false
    end
  end

  @spec size(Btree.t()) :: non_neg_integer

  def size(btree), do: Enum.count(btree)

  @spec select(Btree.t(), [CubDB.select_option()]) :: Enumerable.t()

  def select(btree, options) when is_list(options) do
    check_legacy_select_options!(options)

    min_key =
      case Keyword.fetch(options, :min_key) do
        {:ok, key} ->
          {key, Keyword.get(options, :min_key_inclusive, true)}

        :error ->
          nil
      end

    max_key =
      case Keyword.fetch(options, :max_key) do
        {:ok, key} ->
          {key, Keyword.get(options, :max_key_inclusive, true)}

        :error ->
          nil
      end

    reverse = Keyword.get(options, :reverse, false)

    Btree.key_range(btree, min_key, max_key, reverse)
  end

  @spec check_legacy_select_options!(Keyword.t()) :: :ok

  defp check_legacy_select_options!(options) do
    if Keyword.has_key?(options, :reduce) do
      raise "select/2 does not have a :reduce option anymore. Use Enum.reduce on the returned lazy stream instead."
    end

    if Keyword.has_key?(options, :pipe) do
      raise "select/2 does not have a :pipe option anymore. Pipe the returned lazy stream into functions in Stream or Enum instead."
    end

    :ok
  end
end
