defmodule CubDB.Store.ReaderTest do
  use ExUnit.Case, async: true

  alias CubDB.Btree
  alias CubDB.Reader
  alias CubDB.Store

  defmodule TestCaller do
    use GenServer

    def start_link do
      GenServer.start_link(__MODULE__, [], [])
    end

    def run(pid, btree, operation) do
      GenServer.call(pid, {:run, btree, operation})
    end

    def init(_) do
      {:ok, nil}
    end

    def handle_call({:run, btree, operation}, from, state) do
      Reader.run(btree, from, operation)
      {:noreply, state}
    end
  end

  setup do
    {:ok, store} = Store.TestStore.create()

    entries = [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7]

    btree =
      Enum.reduce(entries, Btree.new(store), fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    {:ok, entries: entries, btree: btree}
  end

  test "perform/2 performs :get", %{btree: btree} do
    assert 4 = Reader.perform(btree, {:get, :d, 42})
    assert 42 = Reader.perform(btree, {:get, :z, 42})

    btree = Btree.insert(btree, :n, nil)
    assert Reader.perform(btree, {:get, :n, 42}) == nil
  end

  test "perform/2 performs :get_multi", %{btree: btree} do
    assert %{c: 3, d: 4} = Reader.perform(btree, {:get_multi, [:c, :d, :z]})
  end

  test "perform/2 performs :fetch", %{btree: btree} do
    assert {:ok, 3} = Reader.perform(btree, {:fetch, :c})

    assert :error = Reader.perform(btree, {:fetch, :z})
  end

  test "perform/2 performs :has_key?", %{btree: btree} do
    assert Reader.perform(btree, {:has_key?, :c})
    refute Reader.perform(btree, {:has_key?, :z})
  end

  test "perform/2 performs :select", %{btree: btree, entries: entries} do
    assert {:ok, ^entries} = Reader.perform(btree, {:select, []})
  end

  test "perform/2 performs :select with :min_key and :max_key", %{btree: btree} do
    assert {:ok, [b: 2, c: 3, d: 4]} = Reader.perform(btree, {:select, min_key: :b, max_key: :d})
  end

  test "perform/2 performs :select with :filter and :map", %{btree: btree} do
    assert {:ok, [2, 4, 6]} =
             Reader.perform(
               btree,
               {:select,
                pipe: [
                  filter: fn {_, value} -> rem(value, 2) == 0 end,
                  map: fn {_, value} -> value end
                ]}
             )
  end

  test "perform/2 performs :select with :reduce", %{btree: btree} do
    assert {:ok, 28} =
             Reader.perform(
               btree,
               {:select,
                pipe: [map: fn {_, value} -> value end], reduce: fn value, sum -> sum + value end}
             )

    assert {:ok, 28} =
             Reader.perform(
               btree,
               {:select, reduce: {0, fn {_, value}, sum -> sum + value end}}
             )
  end

  test "perform/2 performs :select and reports errors", %{btree: btree} do
    assert {:error, %ArithmeticError{}} =
             Reader.perform(
               btree,
               {:select, reduce: fn _, _ -> raise(ArithmeticError, message: "boom") end}
             )
  end

  test "perform/2 performs :select with :reverse", %{btree: btree, entries: entries} do
    reverse_entries = Enum.reverse(entries)

    assert {:ok, ^reverse_entries} = Reader.perform(btree, {:select, [reverse: true]})
  end

  test "perform/2 performs :select with :take and :drop", %{btree: btree} do
    assert {:ok, [c: 3, d: 4]} = Reader.perform(btree, {:select, pipe: [take: 4, drop: 2]})
  end

  test "perform/2 performs :select with :take_while and :drop_while", %{btree: btree} do
    assert {:ok, [c: 3, d: 4]} =
             Reader.perform(
               btree,
               {:select,
                pipe: [take_while: fn {_, v} -> v < 5 end, drop_while: fn {_, v} -> v < 3 end]}
             )
  end

  test "perform/2 performs :select with invalid :pipe", %{btree: btree} do
    assert {:error, _} = Reader.perform(btree, {:select, pipe: [xxx: 123]})
  end

  test "run/3 performs the read and replies to the caller GenServer", %{btree: btree} do
    {:ok, pid} = TestCaller.start_link()
    assert {:ok, [c: 3, d: 4]} = TestCaller.run(pid, btree, {:select, pipe: [take: 4, drop: 2]})
  end
end
