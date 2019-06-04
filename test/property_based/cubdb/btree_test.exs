defmodule PropertyBased.BtreeTest do
  use ExUnit.Case
  use Quixir

  alias CubDB.Store
  alias CubDB.Btree
  alias Store.Utils

  @leaf Btree.__leaf__

  @tag property_based: true
  test "a Btree grows and shrinks" do
    ptest [
      cap: int(min: 2, max: 32),
      tuples: list(max: 100,
                   of: tuple(like: {
                               string(max: 8, chars: :printable),
                               string(max: 10)
                             }))
    ], repeat_for: 50 do
      store = Store.MemMap.new
      tree = Btree.new(store, cap)
      tree = Enum.reduce(tuples, tree, fn {key, value}, t ->
        previous_count = Enum.count(t)
        t = Btree.insert(t, key, value)
        assert Enum.count(t) >= previous_count
        assert Btree.lookup(t, key) == value
        t
      end)

      a = Enum.into(tree, [])
      b = tuples
          |> Enum.reverse
          |> Enum.uniq_by(&(elem(&1, 0)))
          |> List.keysort(0)
      assert a == b

      tree = Enum.reduce(tuples, tree, fn {key, _}, t ->
        previous_count = Enum.count(t)
        t = Btree.delete(t, key)

        assert Enum.count(t) <= previous_count

        e = Enum.into(t, [])
        assert e == e |> List.keysort(0)

        assert Btree.lookup(t, key) == nil
        t
      end)

      Btree.commit(tree)
      assert {:Btree, 0, {@leaf, []}} = Utils.debug(tree.store)
    end
  end
end
