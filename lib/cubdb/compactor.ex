defmodule CubDB.Compactor do
  @moduledoc false

  # The `CubDB.Compactor` module takes care of the compaction operation, to
  # reclaim disk space. It does so by loading all entries in a new store (using
  # the fast algorithm provided by `Btree.load/2`). This will cause the new
  # store to only contain the entries reachable from the current root.
  #
  # Compaction runs in the background and does not block read/write operations.
  # This module only takes care of loading the compacted entries in the new
  # store, and then reports back to the caller. Subsequent steps in the
  # compaction process (catching up with updates performed during compaction,
  # swapping the store, cleaning up old files) are performed by other modules
  # and coordinated by the main db process.

  use Task

  alias CubDB.Btree
  alias CubDB.Store

  @spec run(pid, Btree.t(), Store.File.t()) :: :ok

  def run(caller, btree, store) do
    compacted_btree = Btree.load(btree, store)
    Btree.sync(compacted_btree)
    send(caller, {:compaction_completed, self(), btree, compacted_btree})
  end
end
