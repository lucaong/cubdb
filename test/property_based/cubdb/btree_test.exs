defmodule PropertyBased.BtreeTest do
  use ExUnit.Case
  use Quixir

  alias CubDB.Store
  alias CubDB.Btree
  alias TestHelper.Btree.Utils

  @leaf Btree.__leaf__()

  @tag property_based: true
  test "a Btree grows and shrinks" do
    ptest [
      cap: int(min: 2, max: 32),
      tuples:
      list(
        max: 100,
        of:
        tuple(
          like: {
            string(max: 8, chars: :printable),
            string(max: 10)
          }
        )
      )
    ], repeat_for: 50 do
      store = Store.TestStore.new()
      tree = Btree.new(store, cap)

      tree =
        Enum.reduce(tuples, tree, fn {key, value}, t ->
          new_tree = Btree.insert(t, key, value) |> Btree.commit()
          assert Enum.count(new_tree) >= Enum.count(t)
          assert new_tree.dirt > t.dirt
          assert {:ok, ^value} = Btree.fetch(new_tree, key)
          new_tree
        end)

      a = Enum.into(tree, [])

      b =
        tuples
        |> Enum.reverse()
        |> Enum.uniq_by(&elem(&1, 0))
        |> List.keysort(0)

      assert a == b

      tree =
        Enum.reduce(tuples, tree, fn {key, _}, t ->
          previous_count = Enum.count(t)
          previous_dirt_factor = Btree.dirt_factor(t)

          t =
            if rem(previous_count, 2) == 0,
              do: Btree.delete(t, key),
              else: Btree.mark_deleted(t, key)

          t = Btree.commit(t)

          assert Enum.count(t) <= previous_count
          assert Btree.dirt_factor(t) >= previous_dirt_factor

          e = Enum.into(t, [])
          assert e == e |> List.keysort(0)

          assert Btree.fetch(t, key) == :error
          t
        end)

        compacted = Btree.load(tree, Store.TestStore.new(), cap)

        assert Enum.to_list(tree) == Enum.to_list(compacted)
        assert {:Btree, 0, {@leaf, []}} = Utils.debug(compacted.store)
      end
  end

  @tag property_based: true
  test "a Btree gets updated as expected" do
    ptest [
      cap: int(min: 2, max: 32),
      tuples:
      list(
        max: 100,
        of:
        tuple(
          like: {
            string(max: 5, chars: :ascii),
            string(max: 5, chars: :ascii)
          }
        )
      )
    ], repeat_for: 100 do
      store = Store.TestStore.new()
      tree = Btree.new(store, cap)
      inserts = tuples |> Enum.map(fn tuple -> {:insert, tuple} end)
      deletes = tuples |> Enum.map(fn tuple -> {:delete, tuple} end)
      delmarks = tuples |> Enum.map(fn tuple -> {:mark_deleted, tuple} end)
      operations = inserts |> Enum.concat(deletes) |> Enum.concat(delmarks) |> Enum.shuffle()

      tree = Enum.reduce(operations, tree, fn {operation, {key, value}}, tree ->
        case operation do
          :insert ->
            tree = Btree.insert(tree, key, value)
            assert {:ok, ^value} = Btree.fetch(tree, key)
            tree

          :delete ->
            tree = Btree.delete(tree, key)
            assert :error = Btree.fetch(tree, key)
            tree

          :mark_deleted ->
            tree = Btree.mark_deleted(tree, key)
            assert :error = Btree.fetch(tree, key)
            tree
        end
      end)

      expectations = Enum.reduce(operations, %{}, fn {operation, {key, value}}, map ->
        case operation do
          :insert ->
            Map.put(map, key, {:ok, value})

          :delete ->
            Map.put(map, key, :error)

          :mark_deleted ->
            Map.put(map, key, :error)
        end
      end) |> Enum.to_list()

      Enum.each(expectations, fn {key, expected} ->
        assert ^expected = Btree.fetch(tree, key)
      end)
    end
  end
end
