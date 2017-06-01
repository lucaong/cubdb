defmodule CubDB.Store.MemMapTest do
  use CubDB.StoreExamples, async: true

  setup do: {:ok, store: CubDB.Store.MemMap.new}
end
