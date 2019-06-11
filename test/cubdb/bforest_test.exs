defmodule CubDB.BforestTest do
  use ExUnit.Case

  alias CubDB.Store
  alias CubDB.Btree
  alias CubDB.Bforest
  doctest Bforest

  setup do
    trees = Enum.map(1..3, fn n ->
      store = Store.MemMap.new
      Btree.new(store)
      |> Btree.insert(:number, n)
      |> Btree.insert(n, n)
    end)
    forest = trees
             |> Enum.reverse
             |> Enum.reduce(nil, fn (tree, forest) ->
               Bforest.new(tree, forest)
             end)
    {:ok, trees: trees, forest: forest}
  end

  test "insert/3 inserts key/value in the live tree", %{forest: forest} do
    forest = Bforest.insert(forest, :foo, "bar")
    assert {true, "bar"} = Btree.has_key?(forest.btree, :foo)
    assert {false, nil} = Btree.has_key?(forest.bforest.btree, :foo)
    assert {false, nil} = Btree.has_key?(forest.bforest.bforest.btree, :foo)
  end

  test "insert/3 raises if called on non-live bforest", %{forest: forest} do
    frozen_forest = %Bforest{forest | frozen: true}
    assert_raise ArgumentError, fn ->
      Bforest.insert(frozen_forest, :foo, "bar")
    end
  end

  test "delete/2 deletes key/value in the live tree", %{forest: forest} do
    forest = Bforest.delete(forest, :number)
    assert {false, nil} = Btree.has_key?(forest.btree, :number)
    assert {true, 2} = Btree.has_key?(forest.bforest.btree, :number)
    assert {true, 3} = Btree.has_key?(forest.bforest.bforest.btree, :number)
  end

  test "delete/2 raises if called on non-live bforest", %{forest: forest} do
    frozen_forest = %Bforest{forest | frozen: true}
    assert_raise ArgumentError, fn ->
      Bforest.delete(frozen_forest, :foo)
    end
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

  test "compact/2 merges the forest into one compacted tree",
  %{forest: forest} do
    store = Store.MemMap.new
    compacted = Bforest.compact(forest, store)
    assert %Btree{} = compacted
    assert Enum.to_list(compacted) == Enum.to_list(forest)
  end

  test "compact/2 reduces the storage needed by the compacted tree" do
    store = Store.MemMap.new
    tree = Btree.new(store)
    Enum.reduce(1..4, tree, fn x, tree ->
      Btree.insert(tree, x, x)
    end)

    forest = Bforest.new(tree)
    compacted = Bforest.compact(forest, Store.MemMap.new)
    {compacted_header_loc, _} = Store.get_latest_header(compacted.store)
    {tree_header_loc, _} = Store.get_latest_header(store)

    assert compacted_header_loc < tree_header_loc
  end

  test "Bforest implements Enumerable" do
    Protocol.assert_impl!(Enumerable, Bforest)

    trees = Enum.map(1..4, fn n ->
      if n < 4 do
        store = Store.MemMap.new
        key_vals = Enum.zip(n..(n + 2), Stream.cycle([n]))
        Btree.new(store, key_vals)
      else
        Btree.new(Store.MemMap.new)
      end
    end)

    forest = trees
             |> Enum.reverse
             |> Enum.reduce(nil, fn (tree, forest) ->
               Bforest.new(tree, forest)
             end)

    assert Stream.map(forest, &(&1)) |> Enum.to_list ==
      [{1, 1}, {2, 1}, {3, 1}, {4, 2}, {5, 3}]

    assert Stream.zip(forest, Stream.cycle([42])) |> Enum.to_list
      == [{{1, 1}, 42}, {{2, 1}, 42}, {{3, 1}, 42},
          {{4, 2}, 42}, {{5, 3}, 42}]

    assert 5 == Enum.count(forest)
    assert true == Enum.member?(forest, {3, 1})
    assert false == Enum.member?(forest, {3, 2})
  end
end
