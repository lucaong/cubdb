defmodule CubDB.Store.TestStoreTest do
  use CubDB.StoreExamples, async: true

  setup do: {:ok, store: CubDB.Store.TestStore.new}
end
