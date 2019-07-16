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

  use Task

  alias CubDB.Btree

  @type operation :: {:get, any, any} | {:fetch, any} | {:has_key?, any} | {:select, Keyword.t()}

  @spec start_link(GenServer.from(), GenServer.server(), Btree.t(), operation) :: {:ok, pid}

  def start_link(caller, db, btree, read_operation) do
    Task.start_link(__MODULE__, :run, [caller, db, btree, read_operation])
  end

  @spec run(GenServer.from(), GenServer.server(), Btree.t(), operation) :: :ok

  def run(caller, db, btree, {:get, key, default}) do
    case Btree.fetch(btree, key) do
      {:ok, value} -> GenServer.reply(caller, value)
      :error -> GenServer.reply(caller, default)
    end
  after
    send(db, {:check_out_reader, btree})
  end

  def run(caller, db, btree, {:fetch, key}) do
    reply = Btree.fetch(btree, key)
    GenServer.reply(caller, reply)
  after
    send(db, {:check_out_reader, btree})
  end

  def run(caller, db, btree, {:has_key?, key}) do
    case Btree.fetch(btree, key) do
      {:ok, _} -> GenServer.reply(caller, true)
      :error -> GenServer.reply(caller, false)
    end
  after
    send(db, {:check_out_reader, btree})
  end

  def run(caller, db, btree, {:select, options}) do
    reply = select(btree, options)
    GenServer.reply(caller, {:ok, reply})
  rescue
    error -> GenServer.reply(caller, {:error, error})
  after
    send(db, {:check_out_reader, btree})
  end

  @spec select(Btree.t(), Keyword.t()) :: any

  defp select(btree, options) when is_list(options) do
    min_key = Keyword.get(options, :min_key)
    max_key = Keyword.get(options, :max_key)
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
