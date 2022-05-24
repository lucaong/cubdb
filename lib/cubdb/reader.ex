defmodule CubDB.Reader do
  @moduledoc false

  # The `CubDB.Reader` module performs all read operations that involve access
  # to the store. Read operations are performed on the Btree representing a
  # snapshot of the database at the time the read operation was invoked.

  alias CubDB.Btree

  @type operation ::
          {:get, CubDB.key(), CubDB.value()}
          | {:get_multi, [CubDB.key()]}
          | {:fetch, CubDB.key()}
          | {:has_key?, CubDB.key()}
          | {:select, Keyword.t()}

  @spec get(Btree.t(), CubDB.key(), any) :: any

  def get(btree, key, default) do
    case Btree.fetch(btree, key) do
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

  @spec select(Btree.t(), [CubDB.select_option()]) :: any

  def select(btree, options) when is_list(options) do
    pipe = Keyword.get(options, :pipe, [])
    reduce = Keyword.get(options, :reduce)

    key_range = select_stream(btree, options)

    stream =
      Enum.reduce(pipe, key_range, fn
        {:filter, fun}, stream when is_function(fun) -> Stream.filter(stream, fun)
        {:map, fun}, stream when is_function(fun) -> Stream.map(stream, fun)
        {:take, n}, stream when is_integer(n) -> Stream.take(stream, n)
        {:drop, n}, stream when is_integer(n) -> Stream.drop(stream, n)
        {:take_while, fun}, stream when is_function(fun) -> Stream.take_while(stream, fun)
        {:drop_while, fun}, stream when is_function(fun) -> Stream.drop_while(stream, fun)
        op, _ -> raise(ArgumentError, message: "invalid pipe operation #{inspect(op)}")
      end)

    case reduce do
      fun when is_function(fun) -> Enum.reduce(stream, fun)
      {acc, fun} when is_function(fun) -> Enum.reduce(stream, acc, fun)
      nil -> Enum.to_list(stream)
    end
  end

  @spec select_stream(Btree.t(), [CubDB.select_option()]) :: Enumerable.t()

  def select_stream(btree, options) when is_list(options) do
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
end
