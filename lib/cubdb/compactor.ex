defmodule CubDB.Compactor do
  @moduledoc false

  # The `CubDB.Compactor` module takes care of the compaction, to reclaim disk
  # space. It does so by loading all entries in a new store (using the fast
  # algorithm provided by `Btree.load/2`). This will cause the new store to only
  # contain the entries reachable from the current root. This is the so called
  # compaction phase.
  #
  # After compacting, `CubDB.Compactor` also needs to catch up the compacted
  # Btree with updates performed on the live Btree while the compaction was
  # running. The catch up process can be iterated multiple times until the
  # compacted Btree is in sync with the live one.
  #
  # The catch up is performed by iterating through all updates happened after
  # the compaction (or the last catch up) started, using the `Btree.Diff`
  # enumerable to enumerate updates between two Btrees representing two
  # snapshots of the same (live) store.

  alias CubDB.Btree
  alias CubDB.Snapshot
  alias CubDB.Store
  alias CubDB.Tx

  import Btree, only: [value: 1, deleted: 0]

  @spec run(CubDB.server(), Store.File.t()) :: :ok

  def run(db, store) do
    {original_btree, compacted_btree} =
      CubDB.with_snapshot(db, fn %Snapshot{btree: btree} ->
        {btree, compact(btree, store)}
      end)

    send(db, :compaction_completed)

    catch_up(db, compacted_btree, original_btree)

    send(db, :catch_up_completed)
  end

  @spec compact(Btree.t(), Store.t()) :: Btree.t()

  def compact(btree, store) do
    Btree.load(btree, store) |> Btree.sync()
  end

  @spec catch_up(CubDB.server(), Btree.t(), Btree.t()) :: :ok

  def catch_up(db, compacted_btree, original_btree) do
    result =
      CubDB.transaction(db, fn
        %Tx{btree: ^original_btree} = tx ->
          {:commit, %Tx{tx | btree: %{compacted_btree | metadata: tx.btree.metadata}}, :done}

        %Tx{btree: latest_btree} ->
          {:cancel, latest_btree}
      end)

    case result do
      :done ->
        :ok

      latest_btree ->
        compacted_btree = catch_up_iter(compacted_btree, original_btree, latest_btree)
        catch_up(db, compacted_btree, latest_btree)
    end
  end

  @spec catch_up_iter(Btree.t(), Btree.t(), Btree.t()) :: Btree.t()

  def catch_up_iter(compacted_btree, original_btree, latest_btree) do
    diff = Btree.Diff.new(original_btree, latest_btree)

    Enum.reduce(diff, compacted_btree, fn
      {key, value(val: value)}, compacted_btree ->
        Btree.insert(compacted_btree, key, value)

      {key, deleted()}, compacted_btree ->
        Btree.delete(compacted_btree, key)
    end)
  end
end
