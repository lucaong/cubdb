defmodule CubDB.Store.ReaderTest do
  use ExUnit.Case, async: true

  alias CubDB.Btree
  alias CubDB.Reader
  alias CubDB.Store

  setup do
    {:ok, store} = Store.TestStore.create()

    entries = [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7]

    btree =
      Enum.reduce(entries, Btree.new(store), fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    {:ok, entries: entries, btree: btree}
  end

  test "get/3 gets the value of an entry", %{btree: btree} do
    assert 4 = Reader.get(btree, :d, 42)
    assert 42 = Reader.get(btree, :z, 42)

    btree = Btree.insert(btree, :n, nil)
    assert Reader.get(btree, :n, 42) == nil
  end

  test "get_multi/2 gets the value of multiple entries", %{btree: btree} do
    assert %{c: 3, d: 4} = Reader.get_multi(btree, [:c, :d, :z])
  end

  test "fetch/2 returns `{:ok, value}` if the entry exists, `:error` otherwise", %{btree: btree} do
    assert {:ok, 3} = Reader.fetch(btree, :c)

    assert :error = Reader.fetch(btree, :z)
  end

  test "has_key?/2 returns true if the key exists, false otherwise", %{btree: btree} do
    assert Reader.has_key?(btree, :c)
    refute Reader.has_key?(btree, :z)
  end

  test "size/1 returns the number of entries", %{btree: btree} do
    assert 7 = Reader.size(btree)
  end

  test "select/2 selects all entries", %{btree: btree, entries: entries} do
    assert ^entries = Reader.select(btree, [])
  end

  test "select/2 with :min_key and :max_key selects a range of entries", %{btree: btree} do
    assert [b: 2, c: 3, d: 4] = Reader.select(btree, min_key: :b, max_key: :d)
  end

  test "select/2 with :filter and :map selects a range of entries and applies the pipe", %{
    btree: btree
  } do
    assert [2, 4, 6] =
             Reader.select(
               btree,
               pipe: [
                 filter: fn {_, value} -> rem(value, 2) == 0 end,
                 map: fn {_, value} -> value end
               ]
             )
  end

  test "select/2 with :reduce applies the reduction", %{btree: btree} do
    assert 28 =
             Reader.select(
               btree,
               pipe: [map: fn {_, value} -> value end],
               reduce: fn value, sum -> sum + value end
             )

    assert 28 =
             Reader.select(
               btree,
               reduce: {0, fn {_, value}, sum -> sum + value end}
             )
  end

  test "select/2 with :reverse selects in inverse order", %{btree: btree, entries: entries} do
    reverse_entries = Enum.reverse(entries)

    assert ^reverse_entries = Reader.select(btree, reverse: true)
  end

  test "select/2 with :take and :drop takes and drops entries", %{btree: btree} do
    assert [c: 3, d: 4] = Reader.select(btree, pipe: [take: 4, drop: 2])
  end

  test "select/2 with :take_while and :drop_while", %{btree: btree} do
    assert [c: 3, d: 4] =
             Reader.select(
               btree,
               pipe: [take_while: fn {_, v} -> v < 5 end, drop_while: fn {_, v} -> v < 3 end]
             )
  end

  test "perform/2 performs :select with invalid :pipe", %{btree: btree} do
    assert_raise ArgumentError, fn -> Reader.select(btree, pipe: [xxx: 123]) end
  end
end
