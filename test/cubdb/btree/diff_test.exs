defmodule CubDB.Btree.DiffTest do
  use ExUnit.Case

  alias CubDB.Store
  alias CubDB.Btree
  alias CubDB.Btree.Diff

  import TestHelper

  doctest Btree.Diff

  @value Btree.__value__
  @deleted Btree.__deleted__

  test "Diff implements Enumerable" do
    Protocol.assert_impl!(Enumerable, Diff)
  end

  test "iterates through updates between from_btree and to_btree" do
    store = Store.TestStore.new
    from_btree = make_btree(store, [foo: 1, bar: 2, baz: 3], 3)
    to_btree = Enum.reduce([qux: 4, quux: 5], from_btree, fn {key, val}, btree ->
      Btree.insert(btree, key, val)
    end) |> Btree.mark_deleted(:bar)

    to_btree |> Btree.insert(:x, 6) |> Btree.mark_deleted(:baz)

    diff = Diff.new(from_btree, to_btree)

    assert Enum.to_list(diff) == [{:bar, @deleted}, {:quux, {@value, 5}}, {:qux, {@value, 4}}]
  end
end
