defmodule CubDB.CatchUp do
  use Task

  alias CubDB.Btree

  @value Btree.__value__
  @deleted Btree.__deleted__

  def start_link(caller, compacted_btree, original_btree, latest_btree) do
    Task.start_link(__MODULE__, :run, [caller, compacted_btree, original_btree, latest_btree])
  end

  def run(caller, compacted_btree, original_btree, latest_btree) do
    compacted_btree = catch_up(compacted_btree, original_btree, latest_btree)
    send(caller, {:catch_up, compacted_btree, latest_btree})
  end

  defp catch_up(compacted_btree, original_btree, latest_btree) do
    diff = Btree.Diff.new(original_btree, latest_btree)
    Enum.reduce(diff, compacted_btree, fn
      {key, {@value, value}}, compacted_btree -> Btree.insert(compacted_btree, key, value)
      {key, @deleted}, compacted_btree -> Btree.delete(compacted_btree, key)
    end)
  end
end
