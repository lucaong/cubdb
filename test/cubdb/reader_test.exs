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
    assert ^entries = Reader.select(btree, []) |> Enum.to_list()
  end

  test "select/2 with :min_key and :max_key selects a range of entries", %{btree: btree} do
    assert [b: 2, c: 3, d: 4] = Reader.select(btree, min_key: :b, max_key: :d) |> Enum.to_list()
  end

  test "select/2 with :reverse selects in inverse order", %{btree: btree, entries: entries} do
    reverse_entries = Enum.reverse(entries)

    assert ^reverse_entries = Reader.select(btree, reverse: true) |> Enum.to_list()
  end
end
