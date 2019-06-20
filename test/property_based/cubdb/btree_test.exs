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
      store = Store.TestStore.new
      tree = Btree.new(store, cap)
      tree = Enum.reduce(tuples, tree, fn {key, value}, t ->
        new_tree = Btree.insert(t, key, value)
        assert Enum.count(new_tree) >= Enum.count(t)
        assert new_tree.dirt > t.dirt
        assert Btree.lookup(new_tree, key) == value
        new_tree
      end)

      a = Enum.into(tree, [])
      b = tuples
          |> Enum.reverse
          |> Enum.uniq_by(&(elem(&1, 0)))
          |> List.keysort(0)
      assert a == b

      tree = Enum.reduce(tuples, tree, fn {key, _}, t ->
        previous_count = Enum.count(t)
        previous_dirt_factor = Btree.dirt_factor(t)

        t = if rem(previous_count, 2) == 0,
          do: Btree.delete(t, key),
          else: Btree.mark_deleted(t, key)

        assert Enum.count(t) <= previous_count
        assert Btree.dirt_factor(t) >= previous_dirt_factor

        e = Enum.into(t, [])
        assert e == e |> List.keysort(0)

        assert Btree.lookup(t, key) == nil
        t
      end)

      compacted = Btree.load(tree, Store.TestStore.new, cap)

      assert Enum.to_list(tree) == Enum.to_list(compacted)
      assert {:Btree, 0, {@leaf, []}} = Utils.debug(compacted.store)
    end
  end
end
