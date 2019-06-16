defmodule CubDB.Store.CatchUpTest do
  use ExUnit.Case

  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.CatchUp

  test "start_link/4 catches up the compacted tree with the diff updates and reports result" do
    store = Store.MemMap.new

    entries = [foo: 1, bar: 2]
    original_btree = Enum.reduce(entries, Btree.new(store), fn {key, value}, btree ->
      Btree.insert(btree, key, value)
    end)

    more_entries = [baz: 3, qux: 4]
    latest_btree = Enum.reduce(more_entries, original_btree, fn {key, value}, btree ->
      Btree.insert(btree, key, value)
    end)

    compacted_store = Store.MemMap.new
    compacted_btree = Btree.load(original_btree, compacted_store) 

    {:ok, _pid} = CatchUp.start_link(self(), compacted_btree, original_btree, latest_btree)

    assert_receive {:catch_up, catched_up_btree, ^latest_btree}
    assert Enum.to_list(catched_up_btree) == Enum.to_list(latest_btree)
    assert catched_up_btree.store == compacted_store
  end
end
