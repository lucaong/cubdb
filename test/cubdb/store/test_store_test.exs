defmodule CubDB.Store.TestStoreTest do
  use ExUnit.Case, async: true
  use CubDB.StoreExamples

  alias CubDB.Store.TestStore

  setup do
    {:ok, store} = TestStore.create()
    {:ok, store: store}
  end

  test "start_link/0 starts a Store.TestStore" do
    {:ok, store} = TestStore.create()
    assert %TestStore{agent: pid} = store
    assert Process.alive?(pid)
  end

  test "close/1 stops the agent", %{store: store} do
    %TestStore{agent: pid} = store

    assert Process.alive?(pid) == true

    CubDB.Store.close(store)

    assert Process.alive?(pid) == false
  end

  test "open?/1 returns true if the agent is alive, false otherwise", %{store: store} do
    assert CubDB.Store.open?(store) == true

    CubDB.Store.close(store)

    assert CubDB.Store.open?(store) == false
  end
end
