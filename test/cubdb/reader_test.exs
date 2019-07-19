defmodule CubDB.Store.ReaderTest do
  use ExUnit.Case, async: true

  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Reader

  setup do
    store = Store.TestStore.new()

    entries = [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7]

    btree =
      Enum.reduce(entries, Btree.new(store), fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    {:ok, entries: entries, btree: btree}
  end

  test "start_link/4 performs :get, and checks out", %{btree: btree} do
    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:get, :d, 42})
    assert_receive {:test_tag, 4}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:get, :z, 42})
    assert_receive {:test_tag, 42}
    assert_receive {:check_out_reader, ^btree}

    btree = Btree.insert(btree, :n, nil)
    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:get, :n, 42})
    assert_receive {:test_tag, nil}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :fetch, and checks out", %{btree: btree} do
    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:fetch, :c})
    assert_receive {:test_tag, {:ok, 3}}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:fetch, :z})
    assert_receive {:test_tag, :error}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :has_key?, and checks out", %{btree: btree} do
    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:has_key?, :c})
    assert_receive {:test_tag, true}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:has_key?, :z})
    assert_receive {:test_tag, false}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select, and checks out", %{btree: btree, entries: entries} do
    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:select, []})
    assert_receive {:test_tag, {:ok, ^entries}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select with :min_key and :max_key", %{btree: btree} do
    {:ok, _} =
      Reader.start_link(
        {self(), :test_tag},
        self(),
        btree,
        {:select,
         [
           min_key: :b,
           max_key: :d
         ]}
      )

    assert_receive {:test_tag, {:ok, [b: 2, c: 3, d: 4]}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select with :filter and :map", %{btree: btree} do
    {:ok, _} =
      Reader.start_link(
        {self(), :test_tag},
        self(),
        btree,
        {:select,
         [
           pipe: [
             filter: fn {_, value} -> rem(value, 2) == 0 end,
             map: fn {_, value} -> value end
           ]
         ]}
      )

    assert_receive {:test_tag, {:ok, [2, 4, 6]}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select with :reduce", %{btree: btree} do
    {:ok, _} =
      Reader.start_link(
        {self(), :test_tag},
        self(),
        btree,
        {:select,
         [
           pipe: [
             map: fn {_, value} -> value end
           ],
           reduce: fn value, sum -> sum + value end
         ]}
      )

    assert_receive {:test_tag, {:ok, 28}}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} =
      Reader.start_link(
        {self(), :test_tag},
        self(),
        btree,
        {:select,
         [
           reduce: {0, fn {_, value}, sum -> sum + value end}
         ]}
      )

    assert_receive {:test_tag, {:ok, 28}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select and reports errors", %{btree: btree} do
    {:ok, _} =
      Reader.start_link(
        {self(), :test_tag},
        self(),
        btree,
        {:select,
         [
           reduce: fn _, _ -> raise(ArithmeticError, message: "boom") end
         ]}
      )

    assert_receive {:test_tag, {:error, %ArithmeticError{}}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select with :reverse", %{btree: btree, entries: entries} do
    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:select, [reverse: true]})
    reverse_entries = Enum.reverse(entries)
    assert_receive {:test_tag, {:ok, ^reverse_entries}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select with :take and :drop", %{btree: btree} do
    {:ok, _} =
      Reader.start_link(
        {self(), :test_tag},
        self(),
        btree,
        {:select,
         [
           pipe: [take: 4, drop: 2]
         ]}
      )

    assert_receive {:test_tag, {:ok, [c: 3, d: 4]}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select with :take_while and :drop_while", %{btree: btree} do
    {:ok, _} =
      Reader.start_link(
        {self(), :test_tag},
        self(),
        btree,
        {:select,
         [
           pipe: [
             take_while: fn {_, v} -> v < 5 end,
             drop_while: fn {_, v} -> v < 3 end
           ]
         ]}
      )

    assert_receive {:test_tag, {:ok, [c: 3, d: 4]}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs :select with invalid :pipe", %{btree: btree} do
    {:ok, _} =
      Reader.start_link(
        {self(), :test_tag},
        self(),
        btree,
        {:select,
         [
           pipe: [
             xxx: 123
           ]
         ]}
      )

    assert_receive {:test_tag, {:error, _}}
    assert_receive {:check_out_reader, ^btree}
  end
end
