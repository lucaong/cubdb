defmodule PropertyBased.BtreeTest do
  use ExUnit.Case
  use Quixir

  alias CubDB.Store
  alias CubDB.Btree

  @tag property_based: true
  test "a Btree gets updated as expected" do
    ptest [
      cap: int(min: 2, max: 32),
      tuples:
      list(
        max: 200,
        of:
        tuple(
          like: {
            string(max: 8, chars: :ascii),
            string(max: 1000)
          }
        )
      )
    ], repeat_for: 200 do
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

      expected_size = Enum.count(expectations, fn
        {_, {:ok, _}} -> true
        _ -> false
      end)

      assert tree.size == expected_size
    end
  end
end
