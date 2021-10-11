defmodule CubDB.Store.CatchUpTest do
  use ExUnit.Case, async: true

  alias CubDB.Btree
  alias CubDB.CatchUp
  alias CubDB.Store

  test "run/4 catches up the compacted tree with the diff updates and reports result" do
    {:ok, store} = Store.TestStore.create()

    entries = [foo: 1, bar: 2]

    original_btree =
      Enum.reduce(entries, Btree.new(store), fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    more_entries = [baz: 3, qux: 4]

    latest_btree =
      Enum.reduce(more_entries, original_btree, fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    {:ok, compacted_store} = Store.TestStore.create()
    compacted_btree = Btree.load(original_btree, compacted_store)

    CatchUp.run(self(), compacted_btree, original_btree, latest_btree)

    assert_receive {:catch_up, pid, catched_up_btree, ^latest_btree}
    assert pid == self()
    assert Enum.to_list(catched_up_btree) == Enum.to_list(latest_btree)
    assert catched_up_btree.store == compacted_store
  end
end
