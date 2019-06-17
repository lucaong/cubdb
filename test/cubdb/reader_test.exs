defmodule CubDB.Store.ReaderTest do
  use ExUnit.Case

  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Reader

  test "start_link/4 performs the read operation, returns the result, and checks out" do
    store = Store.TestStore.new

    entries = [foo: 1, bar: 2, baz: 3]
    btree = Enum.reduce(entries, Btree.new(store), fn {key, value}, btree ->
      Btree.insert(btree, key, value)
    end)

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:get, :bar})
    assert_receive {:test_tag, 2}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:has_key?, :bar})
    assert_receive {:test_tag, {true, 2}}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, :size)
    assert_receive {:test_tag, 3}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:select, []})
    assert_receive {:test_tag, {:ok, [bar: 2, baz: 3, foo: 1]}}
    assert_receive {:check_out_reader, ^btree}
  end

  test "start_link/4 performs the select operation with options" do
    store = Store.TestStore.new

    entries = [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7]
    btree = Enum.reduce(entries, Btree.new(store), fn {key, value}, btree ->
      Btree.insert(btree, key, value)
    end)

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:select, [
      from_key: :b,
      to_key: :f,
      pipe: [
        take: 4,
        filter: fn {_, value} -> rem(value, 2) == 0 end,
        map: fn {_, value} -> value end
      ],
      reduce: fn value, sum -> sum + value end
    ]})
    assert_receive {:test_tag, {:ok, 6}}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:select, [
      pipe: [
        map: fn {_, value} -> value end
      ],
      reduce: {100, fn value, sum -> sum + value end}
    ]})
    assert_receive {:test_tag, {:ok, 128}}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:select, [
      reduce: fn _, _ -> raise(ArithmeticError, message: "boom") end
    ]})
    assert_receive {:test_tag, {:error, %ArithmeticError{}}}
    assert_receive {:check_out_reader, ^btree}

    {:ok, _} = Reader.start_link({self(), :test_tag}, self(), btree, {:select, [reverse: true]})
    reverse_entries = Enum.reverse(entries)
    assert_receive {:test_tag, {:ok, ^reverse_entries}}
    assert_receive {:check_out_reader, ^btree}
  end
end
