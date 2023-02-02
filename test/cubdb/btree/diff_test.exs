defmodule CubDB.Btree.DiffTest do
  use ExUnit.Case

  alias CubDB.Btree
  alias CubDB.Btree.Diff
  alias CubDB.Store

  import Btree, only: [value: 1, deleted: 0]
  import TestHelper

  doctest Btree.Diff

  test "Diff implements Enumerable" do
    Protocol.assert_impl!(Enumerable, Diff)
  end

  test "new/2 returns a Diff" do
    {:ok, store} = Store.TestStore.create()
    from_btree = make_btree(store, [foo: 1, bar: 2, baz: 3], 3)

    to_btree =
      Enum.reduce([qux: 4, quux: 5], from_btree, fn {key, val}, btree ->
        Btree.insert(btree, key, val)
      end)

    assert %Diff{from_btree: ^from_btree, to_btree: ^to_btree} = Diff.new(from_btree, to_btree)
  end

  test "new/2 raises an error if from_btree and to_btree do not share the same store" do
    {:ok, from_store} = Store.TestStore.create()
    {:ok, to_store} = Store.TestStore.create()
    from_btree = make_btree(from_store, [foo: 1, bar: 2, baz: 3], 3)
    to_btree = make_btree(to_store, [foo: 1, bar: 2, baz: 3, qux: 4], 3)

    assert_raise ArgumentError, fn ->
      Diff.new(from_btree, to_btree)
    end
  end

  test "iterates through updates between from_btree and to_btree" do
    {:ok, store} = Store.TestStore.create()
    from_btree = make_btree(store, [foo: 1, bar: 2, baz: 3], 3)

    to_btree =
      Enum.reduce([qux: 4, quux: 5], from_btree, fn {key, val}, btree ->
        Btree.insert(btree, key, val)
      end)
      |> Btree.mark_deleted(:bar)

    to_btree |> Btree.insert(:x, 6) |> Btree.mark_deleted(:baz)

    diff = Diff.new(from_btree, to_btree)

    assert Enum.to_list(diff) == [
             {:bar, deleted()},
             {:quux, value(val: 5)},
             {:qux, value(val: 4)}
           ]
  end

  test "Enumerable.count, Enumerable.member?, and Enumerable.slice return {:error, __MODULE__}" do
    {:ok, store} = Store.TestStore.create()
    from_btree = make_btree(store, [foo: 1, bar: 2, baz: 3], 3)

    to_btree =
      Enum.reduce([qux: 4, quux: 5], from_btree, fn {key, val}, btree ->
        Btree.insert(btree, key, val)
      end)

    diff = Diff.new(from_btree, to_btree)

    assert {:error, Enumerable.CubDB.Btree.Diff} = Enumerable.CubDB.Btree.Diff.count(diff)
    assert {:error, Enumerable.CubDB.Btree.Diff} = Enumerable.CubDB.Btree.Diff.member?(diff, :x)
    assert {:error, Enumerable.CubDB.Btree.Diff} = Enumerable.CubDB.Btree.Diff.count(diff)
  end
end
