defmodule CubDB.Store.TestStoreTest do
  use CubDB.StoreExamples, async: true

  setup do: {:ok, store: CubDB.Store.TestStore.new}

  test "new/0 returns a Store.TestStore" do
    store = CubDB.Store.TestStore.new
    assert %CubDB.Store.TestStore{agent: pid} = store
    assert Process.alive?(pid)
  end

  test "close/1 stops the agent", %{store: store} do
    %CubDB.Store.TestStore{agent: pid} = store

    assert Process.alive?(pid) == true

    CubDB.Store.close(store)

    assert Process.alive?(pid) == false
  end
end
