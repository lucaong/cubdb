defmodule CubDB.CatchUp do
  @moduledoc false

  # The `CubDB.CatchUp` module takes care of catching up a compacted Btree with
  # updates performed on the live Btree during a compaction process. The
  # catch-up process is ran in the background, does not block read/write
  # operations, and can be started multiple times until the compacted Btree is
  # in sync with the live one.
  #
  # The catch-up is performed by iterating through all updates happened after
  # the compaction (or the last catch-up) started, using the `Btree.Diff`
  # enumerable to enumerate updates between two Btrees representing two
  # snapshots of the same (live) store.

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
