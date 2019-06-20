defmodule CubDB.Reader do
  @moduledoc false

  use Task

  alias CubDB.Btree

  @type operation :: {:get, any} | {:fetch, any} | {:select, Keyword.t()} | :size

  @spec start_link(GenServer.from(), GenServer.server(), Btree.t(), operation) :: {:ok, pid}

  def start_link(caller, db, btree, read_operation) do
    Task.start_link(__MODULE__, :run, [caller, db, btree, read_operation])
  end

  @spec run(GenServer.from(), GenServer.server(), Btree.t(), operation) :: :ok

  def run(caller, db, btree, {:get, key}) do
    value = Btree.lookup(btree, key)
    GenServer.reply(caller, value)
  after
    send(db, {:check_out_reader, btree})
  end

  def run(caller, db, btree, {:fetch, key}) do
    case Btree.has_key?(btree, key) do
      {true, value} -> GenServer.reply(caller, {:ok, value})
      {false, _} -> GenServer.reply(caller, :error)
    end
  after
    send(db, {:check_out_reader, btree})
  end

  def run(caller, db, btree, {:has_key?, key}) do
    reply = elem(Btree.has_key?(btree, key), 0)
    GenServer.reply(caller, reply)
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

  def run(caller, db, btree, :size) do
    size = Enum.count(btree)
    GenServer.reply(caller, size)
  after
    send(db, {:check_out_reader, btree})
  end

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
