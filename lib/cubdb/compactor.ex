defmodule CubDB.Compactor do
  use Task

  alias CubDB.Btree
  alias CubDB.Store

  @spec start_link(pid, Btree.t(), Store.File.t()) :: {:ok, pid}

  def start_link(caller, btree, store) do
    Task.start_link(__MODULE__, :run, [caller, btree, store])
  end

  @spec run(pid, Btree.t(), Store.File.t()) :: :ok

  def run(caller, btree, store) do
    compacted_btree = Btree.load(btree, store)
    send(caller, {:compaction_completed, btree, compacted_btree})
  end
end
