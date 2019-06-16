defmodule CubDB.Reader do
  use Task

  alias CubDB.Btree

  @spec start_link(GenServer.from(), GenServer.server(), %Btree{}, {atom, any} | atom) ::
          {:ok, pid}

  def start_link(caller, db, btree, read_operation) do
    Task.start_link(__MODULE__, :run, [caller, db, btree, read_operation])
  end

  @spec run(GenServer.from(), GenServer.server(), %Btree{}, {atom, any} | atom) :: :ok

  def run(caller, db, btree, {:get, key}) do
    value = Btree.lookup(btree, key)
    GenServer.reply(caller, value)
  after
    send(db, {:check_out_reader, btree})
  end

  def run(caller, db, btree, {:has_key?, key}) do
    reply = Btree.has_key?(btree, key)
    GenServer.reply(caller, reply)
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

  def run(caller, db, btree, :size) do
    size = Enum.count(btree)
    GenServer.reply(caller, size)
  after
    send(db, {:check_out_reader, btree})
  end

  defp select(btree, options) when is_list(options) do
    from_key = Keyword.get(options, :from_key)
    to_key = Keyword.get(options, :to_key)
    pipe = Keyword.get(options, :pipe, [])
    reduce = Keyword.get(options, :reduce)

    key_range = Btree.key_range(btree, from_key, to_key)

    stream =
      Enum.reduce(pipe, key_range, fn
        {:filter, fun}, stream when is_function(fun) -> Stream.filter(stream, fun)
        {:map, fun}, stream when is_function(fun) -> Stream.map(stream, fun)
        {:take, n}, stream when is_integer(n) -> Stream.take(stream, n)
        op, _ -> raise(ArgumentError, message: "invalid pipe operation #{inspect(op)}")
      end)

    case reduce do
      fun when is_function(fun) -> Enum.reduce(stream, fun)
      {acc, fun} when is_function(fun) -> Enum.reduce(stream, acc, fun)
      nil -> Enum.to_list(stream)
    end
  end
end
