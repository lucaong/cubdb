defmodule CubDB.Reader do
  @moduledoc false

  # The `CubDB.Reader` module performs all read operations that involve access
  # to the store. Each read operation is ran in its own process, as a `Task`, so
  # that read operations can run concurrently. Read operations are performed on
  # the Btree representing a snapshot of the database at the time the read
  # operation was invoked.
  #
  # At the end of each read operation, a `{:check_out_reader, btree}` message is
  # sent to the main `db` process. That allows the main process to keep track of
  # which files are still referenced by readers, so that clean-up of old files
  # after a compaction can be delayed until no more `Reader` processes reference
  # them.

  alias CubDB.Btree

  @type operation :: {:get, any, any} | {:fetch, any} | {:has_key?, any} | {:select, Keyword.t()}

  @spec run(Btree.t(), GenServer.from(), operation) :: :ok

  def run(btree, caller, operation) do
    GenServer.reply(caller, perform(btree, operation))
  end

  @spec perform(Btree.t(), operation) :: any

  def perform(btree, {:get, key, default}) do
    case Btree.fetch(btree, key) do
      {:ok, value} -> value
      :error -> default
    end
  end

  def perform(btree, {:fetch, key}) do
    Btree.fetch(btree, key)
  end

  def perform(btree, {:has_key?, key}) do
    case Btree.fetch(btree, key) do
      {:ok, _} -> true
      :error -> false
    end
  end

  def perform(btree, {:select, options}) do
    {:ok, select(btree, options)}
  rescue
    error -> {:error, error}
  end

  @spec select(Btree.t(), Keyword.t()) :: any

  defp select(btree, options) when is_list(options) do
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

    pipe = Keyword.get(options, :pipe, [])
    reduce = Keyword.get(options, :reduce)
    reverse = Keyword.get(options, :reverse, false)

    key_range = Btree.key_range(btree, min_key, max_key, reverse)

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
end
