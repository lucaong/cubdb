defmodule CubDB.Btree.KeyRangeTest do
  use ExUnit.Case

  alias CubDB.Store
  alias CubDB.Btree
  alias CubDB.Btree.KeyRange

  doctest Btree.KeyRange

  test "KeyRange implements Enumerable" do
    Protocol.assert_impl!(Enumerable, KeyRange)
  end

  test "iterates values with keys between from and to" do
    entries = [
      foo: 1,
      bar: 2,
      baz: 3,
      qux: 4,
      quux: 5,
      xxx: 6,
      yyy: 7,
      quuux: 8
    ]

    store = Store.MemMap.new
    btree = Btree.new(store, entries, 3)

    for {from, to} <- [{nil, nil}, {:bar, :qux}, {:ba, :zz}, {nil, :qux}, {:yy, :zz}, {:baz, nil}, {:c, :a}] do
      key_range = KeyRange.new(btree, from, to)
      expected_entries =
        entries
        |> Enum.filter(fn {key, _} ->
          (from == nil or key >= from) and (to == nil or key <= to)
        end)
        |> List.keysort(0)

      # assert Enum.count(key_range) == length(expected_entries)
      assert Enum.into(key_range, []) == expected_entries
      assert Stream.map(key_range, &(&1)) |> Enum.to_list == expected_entries
      assert Stream.zip(key_range, entries) |> Enum.to_list ==
        Enum.zip(expected_entries, entries)
    end
  end

  test "Enum.member/2 returns false if key is outside of range, or not in the btree, and true otherwise" do
    entries = [a: 1, b: 2, c: 3, d: 4]
    store = Store.MemMap.new
    btree = Btree.new(store, entries, 3)

    assert Enum.member?(KeyRange.new(btree, :b, :c), {:a, 1}) == false
    assert Enum.member?(KeyRange.new(btree, :b, :z), {:e, 1}) == false
    assert Enum.member?(KeyRange.new(btree, :b, :z), {:a, 0}) == false
    assert Enum.member?(KeyRange.new(btree, nil, nil), 123) == false

    assert Enum.member?(KeyRange.new(btree, :b, :z), {:c, 3}) == true
    assert Enum.member?(KeyRange.new(btree, :b, :nil), {:d, 4}) == true
    assert Enum.member?(KeyRange.new(btree, :nil, :b), {:b, 2}) == true
  end
end
