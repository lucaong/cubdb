defmodule CubDB.CatchUp do
  @moduledoc false

  use Task

  alias CubDB.Btree

  @value Btree.__value__()
  @deleted Btree.__deleted__()

  @spec start_link(pid, Btree.t(), Btree.t(), Btree.t()) :: {:ok, pid}

  def start_link(caller, compacted_btree, original_btree, latest_btree) do
    Task.start_link(__MODULE__, :run, [caller, compacted_btree, original_btree, latest_btree])
  end

  @spec run(pid, Btree.t(), Btree.t(), Btree.t()) :: :ok

  def run(caller, compacted_btree, original_btree, latest_btree) do
    compacted_btree = catch_up(compacted_btree, original_btree, latest_btree)
    send(caller, {:catch_up, compacted_btree, latest_btree})
  end

  defp catch_up(compacted_btree, original_btree, latest_btree) do
    diff = Btree.Diff.new(original_btree, latest_btree)

    Enum.reduce(diff, compacted_btree, fn
      {key, {@value, value}}, compacted_btree ->
        Btree.insert(compacted_btree, key, value)
      {key, @deleted}, compacted_btree ->
        Btree.delete(compacted_btree, key)
    end) |> Btree.commit
  end
end
