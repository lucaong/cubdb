defmodule CubDB.Btree.KeyRangeTest do
  use ExUnit.Case

  alias CubDB.Store
  alias CubDB.Btree
  alias CubDB.Btree.KeyRange

  import TestHelper

  doctest Btree.KeyRange

  test "KeyRange implements Enumerable" do
    Protocol.assert_impl!(Enumerable, KeyRange)
  end

  test "Enumerable.KeyRange.reduce/3 iterates entries with keys between min_key and max_key" do
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

    store = Store.TestStore.new()
    btree = make_btree(store, entries, 3)

    for {min, max} <- [
          {nil, nil},
          {:bar, :qux},
          {:ba, :zz},
          {nil, :qux},
          {:yy, :zz},
          {:baz, nil},
          {:c, :a}
        ],
        min_incl <- [:included, :excluded],
        max_incl <- [:included, :excluded] do
      min_key = if min == nil, do: min, else: {min, min_incl}
      max_key = if max == nil, do: max, else: {max, max_incl}
      key_range = KeyRange.new(btree, min_key, max_key)

      expected_entries =
        entries
        |> Enum.filter(fn {key, _} ->
          (min_key == nil || (min_incl == :included && key >= min) || key > min) &&
            (max_key == nil || (max_incl == :included && key <= max) || key < max)
        end)
        |> List.keysort(0)

      assert Enum.to_list(key_range) == expected_entries
      assert Enum.count(key_range) == length(expected_entries)
      assert Stream.map(key_range, & &1) |> Enum.to_list() == expected_entries

      assert Stream.zip(key_range, entries) |> Enum.to_list() ==
               Enum.zip(expected_entries, entries)
    end
  end

  test "Enumerable.KeyRange.reduce/3 iterates entries in reverse order if reverse is true" do
    entries = [
      a: 1,
      b: 2,
      c: 3,
      d: 4,
      e: 5,
      f: 6,
      g: 7,
      h: 8
    ]

    store = Store.TestStore.new()
    btree = make_btree(store, entries, 3)

    key_range = KeyRange.new(btree, {:b, :included}, {:g, :included}, true)

    assert Enum.to_list(key_range) == [g: 7, f: 6, e: 5, d: 4, c: 3, b: 2]
  end

  test "Enum.member/2 returns false if key is outside of range, or not in the btree, and true otherwise" do
    entries = [a: 1, b: 2, c: 3, d: 4]
    store = Store.TestStore.new()
    btree = make_btree(store, entries, 3)

    assert Enum.member?(KeyRange.new(btree, {:b, :included}, {:c, :included}), {:a, 1}) == false
    assert Enum.member?(KeyRange.new(btree, {:b, :included}, {:z, :included}), {:e, 1}) == false
    assert Enum.member?(KeyRange.new(btree, {:b, :included}, {:z, :included}), {:a, 0}) == false
    assert Enum.member?(KeyRange.new(btree, {:a, :excluded}, {:z, :included}), {:a, 1}) == false
    assert Enum.member?(KeyRange.new(btree, {:a, :included}, {:c, :excluded}), {:c, 3}) == false
    assert Enum.member?(KeyRange.new(btree, nil, nil), 123) == false

    assert Enum.member?(KeyRange.new(btree, {:b, :included}, {:z, :included}), {:b, 2}) == true
    assert Enum.member?(KeyRange.new(btree, {:b, :included}, {:c, :included}), {:c, 3}) == true
    assert Enum.member?(KeyRange.new(btree, {:b, :excluded}, {:z, :excluded}), {:c, 3}) == true
    assert Enum.member?(KeyRange.new(btree, {:b, :excluded}, nil), {:d, 4}) == true
    assert Enum.member?(KeyRange.new(btree, nil, {:c, :excluded}), {:b, 2}) == true
  end

  test "Enumerable.Btree.KeyRange.reduce/3 skips nodes marked as deleted" do
    store = Store.TestStore.new()
    tree = make_btree(store, [a: 1, b: 2, c: 3, d: 4], 3) |> Btree.mark_deleted(:b)
    key_range = KeyRange.new(tree, {:a, :included}, {:d, :excluded})
    assert Enum.to_list(key_range) == [a: 1, c: 3]
  end
end
