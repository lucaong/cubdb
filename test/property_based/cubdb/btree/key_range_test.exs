defmodule PropertyBased.Btree.KeyRangeTest do
  use ExUnit.Case
  use Quixir

  alias CubDB.Store
  alias CubDB.Btree.KeyRange

  import TestHelper

  @tag property_based: true
  test "a KeyRange enumerates through a range of keys" do
    ptest [
      cap: int(min: 2, max: 32),
      key_values: list(max: 100,
                   of: tuple(like: {
                               string(max: 8, chars: :printable),
                               string(max: 10, chars: :printable)
                             })),
      from: string(max: 8, chars: :printable),
      to: string(max: 8, chars: :printable)
    ], repeat_for: 50 do
      store = Store.MemMap.new
      btree = make_btree(store, key_values, cap)
      key_range = KeyRange.new(btree, from, to)

      expected_key_values =
        key_values
        |> Enum.reverse
        |> Enum.uniq_by(&(elem(&1, 0)))
        |> Enum.filter(fn {key, _} ->
          (from == nil or key >= from) and (to == nil or key <= to)
        end)
        |> List.keysort(0)

      assert Enum.into(key_range, []) == expected_key_values
    end
  end
end
