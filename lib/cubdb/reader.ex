defmodule CubDB.Reader do
  use Task

  alias CubDB.Btree

  @spec start_link(GenServer.from, %Btree{}, {atom, any} | atom) :: {:ok, pid}

  def start_link(caller, btree, read_operation) do
    Task.start_link(__MODULE__, :run, [caller, btree, read_operation])
  end

  @spec run(GenServer.from, %Btree{}, {atom, any} | atom) :: :ok

  def run(caller, btree, {:get, key}) do
    value = Btree.lookup(btree, key)
    GenServer.reply(caller, value)
  end

  def run(caller, btree, {:has_key?, key}) do
    reply = Btree.has_key?(btree, key)
    GenServer.reply(caller, reply)
  end

  def run(caller, btree, :size) do
    size = Enum.count(btree)
    GenServer.reply(caller, size)
  end
end
