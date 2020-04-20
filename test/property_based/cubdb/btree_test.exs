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
          ],
          repeat_for: 200 do
      {:ok, store} = Store.TestStore.create()
      tree = Btree.new(store, cap)
      inserts = tuples |> Enum.map(fn tuple -> {:insert, tuple} end)
      insert_news = tuples |> Enum.map(fn tuple -> {:insert_new, tuple} end)
      deletes = tuples |> Enum.map(fn tuple -> {:delete, tuple} end)
      delmarks = tuples |> Enum.map(fn tuple -> {:mark_deleted, tuple} end)

      operations =
        inserts
        |> Enum.concat(insert_news)
        |> Enum.concat(deletes)
        |> Enum.concat(delmarks)
        |> Enum.shuffle()

      tree =
        Enum.reduce(operations, tree, fn {operation, {key, value}}, tree ->
          case operation do
            :insert ->
              tree = Btree.insert(tree, key, value)
              assert {:ok, ^value} = Btree.fetch(tree, key)
              tree

            :insert_new ->
              case Btree.fetch(tree, key) do
                {:ok, existing_val} ->
                  assert {:error, :exists} = Btree.insert_new(tree, key, value)
                  assert {:ok, ^existing_val} = Btree.fetch(tree, key)
                  tree

                :error ->
                  tree = Btree.insert_new(tree, key, value)
                  assert tree != {:error, :exists}
                  assert {:ok, ^value} = Btree.fetch(tree, key)
                  tree
              end

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

      expectations =
        Enum.reduce(operations, %{}, fn {operation, {key, value}}, map ->
          case operation do
            :insert ->
              Map.put(map, key, {:ok, value})

            :insert_new ->
              Map.update(map, key, {:ok, value}, fn
                :error -> {:ok, value}
                x -> x
              end)

            :delete ->
              Map.put(map, key, :error)

            :mark_deleted ->
              Map.put(map, key, :error)
          end
        end)
        |> Enum.to_list()

      Enum.each(expectations, fn {key, expected} ->
        assert ^expected = Btree.fetch(tree, key)
      end)

      expected_size =
        Enum.count(expectations, fn
          {_, {:ok, _}} -> true
          _ -> false
        end)

      assert tree.size == expected_size
    end
  end
end
