defmodule CubDB.BforestTest do
  use ExUnit.Case

  alias CubDB.Store
  alias CubDB.Btree
  alias CubDB.Bforest
  doctest Bforest

  setup do
    trees = Enum.map((1..3), fn n ->
      store = Store.MemMap.new
      Btree.new(store)
      |> Btree.insert(:number, n)
      |> Btree.insert(n, n)
    end)
    {:ok, trees: trees, forest: Bforest.new(trees)}
  end

  test "insert/3 inserts key/value in the live tree", %{forest: forest} do
    forest = Bforest.insert(forest, :foo, "bar")
    [live_tree, tree_2, tree_3] = forest.btrees
    assert {true, "bar"} = Btree.has_key?(live_tree, :foo)
    assert {false, nil} = Btree.has_key?(tree_2, :foo)
    assert {false, nil} = Btree.has_key?(tree_3, :foo)
  end

  test "delete/2 deletes key/value in the live tree", %{forest: forest} do
    forest = Bforest.delete(forest, :number)
    [live_tree, tree_2, tree_3] = forest.btrees
    assert {false, nil} = Btree.has_key?(live_tree, :number)
    assert {true, 2} = Btree.has_key?(tree_2, :number)
    assert {true, 3} = Btree.has_key?(tree_3, :number)
  end

  test "has_key?/2 returns {false, nil} if no tree contains the key",
  %{forest: forest} do
    assert {false, nil} = Bforest.has_key?(forest, :non_existing_key)
  end

  test "has_key?/2 returns {true, value} if any tree contains the key",
  %{forest: forest} do
    assert {true, 1} = Bforest.has_key?(forest, :number)
    assert {true, 2} = Bforest.has_key?(forest, 2)
    assert {true, 3} = Bforest.has_key?(forest, 3)
  end

  test "lookup/2 returns nil if no tree contains the key",
  %{forest: forest} do
    assert nil == Bforest.lookup(forest, :non_existing_key)
  end

  test "lookup/2 returns value if any tree contains the key",
  %{forest: forest} do
    assert 1 == Bforest.lookup(forest, :number)
    assert 2 == Bforest.lookup(forest, 2)
    assert 3 == Bforest.lookup(forest, 3)
  end
end
