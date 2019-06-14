defmodule PropertyBased.Btree.DiffTest do
  use ExUnit.Case
  use Quixir

  alias CubDB.Store
  alias CubDB.Btree
  alias CubDB.Btree.Diff

  @value Btree.__value__
  @deleted Btree.__deleted__

  import TestHelper

  @tag property_based: true
  test "Diff enumerates through updates from a btree to a more updated one" do
    ptest [
      cap: int(min: 2, max: 32),
      entries: list(max: 30,
                   of: tuple(like: {
                               string(max: 4, chars: :printable),
                               string(max: 5, chars: :printable)
                             })),
      updates: list(max: 10,
                   of: tuple(like: {
                               string(max: 4, chars: :printable),
                               string(max: 5, chars: :printable)
                             }))
    ], repeat_for: 50 do
      store = Store.MemMap.new
      from_btree = make_btree(store, entries, cap)
      to_btree = Enum.reduce(updates, from_btree, fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

      deletions = Enum.take_random(entries, div(length(entries), 3)) |> Enum.map(fn {key, _} -> {key, @deleted} end)
      to_btree = Enum.reduce(deletions, to_btree, fn {key, _}, btree ->
        Btree.mark_deleted(btree, key)
      end)

      diff = Diff.new(from_btree, to_btree)

      expected_diff =
        updates
        |> Enum.map(fn {key, value} -> {key, {@value, value}} end)
        |> Enum.concat(deletions)
        |> Enum.reverse
        |> Enum.uniq_by(&(elem(&1, 0)))
        |> List.keysort(0)

      assert Enum.to_list(diff) == expected_diff
    end
  end
end
