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
      min_key: string(max: 8, chars: :printable),
      max_key: string(max: 8, chars: :printable)
    ], repeat_for: 50 do
      store = Store.TestStore.new
      btree = make_btree(store, key_values, cap)
      key_range = KeyRange.new(btree, min_key, max_key)
      reverse_key_range = KeyRange.new(btree, min_key, max_key, true)

      expected_key_values =
        key_values
        |> Enum.reverse
        |> Enum.uniq_by(&(elem(&1, 0)))
        |> Enum.filter(fn {key, _} ->
          (min_key == nil or key >= min_key) and (max_key == nil or key <= max_key)
        end)
        |> List.keysort(0)

      assert Enum.to_list(key_range) == expected_key_values
      assert Enum.to_list(reverse_key_range) == expected_key_values |> Enum.reverse()
    end
  end
end
