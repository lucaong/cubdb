defmodule CubDB.Store.WriterTest do
  use ExUnit.Case, async: true

  alias CubDB.Btree
  alias CubDB.Writer
  alias CubDB.Store

  defmodule TestDB do
    use GenServer

    defmodule State do
      defstruct [:btree, :pid]
    end

    def start_link(btree, pid) do
      GenServer.start_link(__MODULE__, [btree, pid])
    end

    def get_btree(pid) do
      GenServer.call(pid, :get_btree, :infinity)
    end

    def init([btree, pid]) do
      {:ok, %State{btree: btree, pid: pid}}
    end

    def handle_call(:start_write, _, state) do
      send(state.pid, :start_write)
      {:reply, state.btree, state}
    end

    def handle_call(:cancel_write, _, state) do
      send(state.pid, :cancel_write)
      {:reply, :ok, state}
    end

    def handle_call({:complete_write, new_btree}, _, state) do
      send(state.pid, {:complete_write, new_btree})
      {:reply, :ok, %State{state | btree: new_btree}}
    end

    def handle_call(:get_btree, _, state) do
      {:reply, state.btree, state}
    end
  end

  describe "when fun returns {btree, result}" do
    test "acquire/2 sends :start_write, calls fun with the Btree, sends {:complete_write, updated_tree} and returns result" do
      {:ok, store} = Store.TestStore.create()
      original_btree = Btree.new(store)
      modified_btree = Btree.insert(original_btree, :x, 1)

      {:ok, db} = TestDB.start_link(original_btree, self())

      assert 123 =
               Writer.acquire(db, fn btree ->
                 assert_receive :start_write
                 assert original_btree == btree

                 {modified_btree, 123}
               end)

      assert_receive {:complete_write, ^modified_btree}
      refute_receive :cancel_write
      assert TestDB.get_btree(db) == modified_btree
    end
  end

  describe "when fun returns {:cancel, result}" do
    test "acquire/2 sends :start_write, calls fun with the Btree, sends :cancel_write and returns result" do
      {:ok, store} = Store.TestStore.create()
      original_btree = Btree.new(store)

      {:ok, db} = TestDB.start_link(original_btree, self())

      assert 123 =
               Writer.acquire(db, fn btree ->
                 assert_receive :start_write
                 assert original_btree == btree

                 {:cancel, 123}
               end)

      assert_receive :cancel_write
      refute_receive {:complete_write, _}
    end
  end

  describe "when fun raises an exception" do
    test "acquire/2 sends :start_write, calls fun with the Btree, sends :cancel_write and reraise exception" do
      {:ok, store} = Store.TestStore.create()
      original_btree = Btree.new(store)

      {:ok, db} = TestDB.start_link(original_btree, self())

      assert_raise RuntimeError, "boom!", fn ->
        Writer.acquire(db, fn btree ->
          assert_receive :start_write
          assert original_btree == btree

          raise "boom!"
        end)
      end

      assert_receive :cancel_write
      refute_receive {:complete_write, _}
    end
  end

  describe "when fun throws a value" do
    test "acquire/2 sends :start_write, calls fun with the Btree, sends :cancel_write and rethrows value" do
      {:ok, store} = Store.TestStore.create()
      original_btree = Btree.new(store)

      {:ok, db} = TestDB.start_link(original_btree, self())

      assert "hello" =
               catch_throw(
                 Writer.acquire(db, fn btree ->
                   assert_receive :start_write
                   assert original_btree == btree

                   throw("hello")
                 end)
               )

      assert_receive :cancel_write
      refute_receive {:complete_write, _}
    end
  end

  describe "when fun exits" do
    test "acquire/2 sends :start_write, calls fun with the Btree, sends :cancel_write and exits again" do
      {:ok, store} = Store.TestStore.create()
      original_btree = Btree.new(store)

      {:ok, db} = TestDB.start_link(original_btree, self())

      assert "hello" =
               catch_exit(
                 Writer.acquire(db, fn btree ->
                   assert_receive :start_write
                   assert original_btree == btree

                   exit("hello")
                 end)
               )

      assert_receive :cancel_write
      refute_receive {:complete_write, _}
    end
  end
end
