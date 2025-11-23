defmodule CubDBTest do
  use ExUnit.Case, async: true
  require TestHelper
  doctest CubDB

  setup do
    {tmp_dir, 0} = System.cmd("mktemp", ["-d"])
    tmp_dir = tmp_dir |> String.trim() |> String.to_charlist()

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "start_link/1 starts and links the process", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
    assert Process.alive?(db) == true

    {:links, links} = Process.info(self(), :links)
    assert Enum.member?(links, db) == true
  end

  test "start_link/1 accepts data_dir as a single string or charlist argument", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir)
    assert Process.alive?(db) == true
    :ok = CubDB.stop(db)

    {:ok, db} = CubDB.start_link(List.to_string(tmp_dir))
    assert Process.alive?(db) == true
  end

  test "start_link/1 accepts a keyword of options and GenServer options", %{tmp_dir: tmp_dir} do
    name = :"#{tmp_dir}"
    assert {:ok, _} = CubDB.start_link(data_dir: tmp_dir, name: name)
    pid = Process.whereis(name)
    assert Process.alive?(pid)
  end

  test "start_link/1 returns error if options are invalid", %{tmp_dir: tmp_dir} do
    TestHelper.trapping_exit do
      assert {:error, _} = CubDB.start_link(data_dir: tmp_dir, auto_compact: "maybe")
    end
  end

  test "start_link/1 returns error if data_dir is missing" do
    assert {:error, _} = CubDB.start_link(foo: nil)
  end

  test "start_link/1 returns error if data_dir cannot be converted into a string" do
    assert {:error, _} = CubDB.start_link(data_dir: {})
  end

  test "start_link/2 accepts data_dir, and a keyword of options and GenServer options", %{
    tmp_dir: tmp_dir
  } do
    name = :"#{tmp_dir}"
    assert {:ok, _pid} = CubDB.start_link(tmp_dir, name: name)
    pid = Process.whereis(name)
    assert Process.alive?(pid)
  end

  test "start/1 starts the process without linking", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start(data_dir: tmp_dir)
    assert Process.alive?(db) == true

    {:links, links} = Process.info(self(), :links)
    assert Enum.member?(links, db) == false
  end

  test "start/1 accepts data_dir as a single string or charlist argument", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start(tmp_dir)
    assert Process.alive?(db) == true
    :ok = CubDB.stop(db)

    {:ok, db} = CubDB.start(List.to_string(tmp_dir))
    assert Process.alive?(db) == true
  end

  test "start/1 accepts a keyword of options and GenServer options", %{tmp_dir: tmp_dir} do
    name = :"#{tmp_dir}"
    assert {:ok, _} = CubDB.start(data_dir: tmp_dir, name: name)
    pid = Process.whereis(name)
    assert Process.alive?(pid)
  end

  test "start/1 returns error if options are invalid", %{tmp_dir: tmp_dir} do
    assert {:error, _} = CubDB.start(data_dir: tmp_dir, auto_compact: "maybe")
  end

  test "start/1 returns error if data_dir is missing" do
    assert {:error, _} = CubDB.start(foo: nil)
  end

  test "start/1 returns error if data_dir cannot be converted into a string" do
    assert {:error, _} = CubDB.start(data_dir: {})
  end

  test "start/1 returns error if another CubDB process is using the same data dir", %{
    tmp_dir: tmp_dir
  } do
    {:ok, _pid} = CubDB.start_link(data_dir: tmp_dir)
    assert {:error, _} = CubDB.start(data_dir: tmp_dir)
  end

  test "start/2 accepts data_dir, and a keyword of options and GenServer options", %{
    tmp_dir: tmp_dir
  } do
    name = :"#{tmp_dir}"
    assert {:ok, _pid} = CubDB.start(tmp_dir, name: name)
    pid = Process.whereis(name)
    assert Process.alive?(pid)
  end

  test "stop/1 cleans up", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_file_sync: false, auto_compact: false)
    CubDB.put_multi(db, Enum.map(1..100, fn n -> {n, n} end))

    :ok = CubDB.compact(db)
    CubDB.stop(db)

    File.rm_rf!(tmp_dir)
    {:ok, db} = CubDB.start_link(tmp_dir, auto_file_sync: false, auto_compact: false)

    assert :ok = CubDB.compact(db)
  end

  test "put/3, get/3, fetch/2, delete/2, has_key?/2, and put_new/3 work as expected", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir)
    key = {:some, arbitrary: "key"}

    assert CubDB.get(db, key) == nil
    assert CubDB.get(db, key, 42) == 42
    assert CubDB.has_key?(db, key) == false

    value = %{some_arbitrary: "value"}
    assert :ok = CubDB.put(db, key, value)

    assert CubDB.get(db, key, 42) == value
    assert {:ok, ^value} = CubDB.fetch(db, key)
    assert CubDB.has_key?(db, key) == true

    assert :ok = CubDB.delete(db, key)
    assert CubDB.get(db, key) == nil
    assert :error = CubDB.fetch(db, key)
    assert CubDB.has_key?(db, key) == false

    assert :ok = CubDB.put_new(db, key, 123)
    assert {:error, :exists} = CubDB.put_new(db, key, 321)
    assert CubDB.get(db, key) == 123

    CubDB.stop(db)
    {:ok, db} = CubDB.start_link(tmp_dir)
    assert [{^key, 123}] = CubDB.select(db) |> Enum.to_list()
  end

  test "put_metadata/3, delete_metadata/2, get_metadata/2 work as expected", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    assert nil == CubDB.get_metadata(db, :foo)
    assert :default == CubDB.get_metadata(db, :foo, :default)
    assert {:error, :key_not_atom} == CubDB.get_metadata(db, "foo", :default)

    assert :ok == CubDB.put_metadata(db, :foo, 123)
    assert {:error, :key_not_atom} == CubDB.put_metadata(db, "foo", 456)
    assert 123 == CubDB.get_metadata(db, :foo, :default)

    assert :ok == CubDB.delete_metadata(db, :foo)
    assert {:error, :key_not_atom} == CubDB.delete_metadata(db, "foo")
    assert :default == CubDB.get_metadata(db, :foo, :default)
  end

  test "delete/2 does not error and does not write to disk when deleting an entry that was not present",
       %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    {:ok, file_stat} = CubDB.current_db_file(db) |> File.stat()

    assert :ok = CubDB.delete(db, :x)

    assert {:ok, ^file_stat} = CubDB.current_db_file(db) |> File.stat()
  end

  test "delete_multi/2 does not error and does not write to disk when deleting an entry that was not present",
       %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    {:ok, file_stat} = CubDB.current_db_file(db) |> File.stat()

    assert :ok = CubDB.delete_multi(db, [:x, :y])

    assert {:ok, ^file_stat} = CubDB.current_db_file(db) |> File.stat()
  end

  test "select/2 works as expected", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    entries = [
      {{:names, 0}, "Ada"},
      {{:names, 2}, "Zoe"},
      {{:names, 1}, "Jay"},
      {:b, 2},
      {:a, 1},
      {:c, 3}
    ]

    :ok = CubDB.put_multi(db, entries)

    result =
      CubDB.select(db,
        min_key: {:names, 0},
        max_key: {:names, 2},
        max_key_inclusive: false
      )
      |> Enum.into([])

    assert [{{:names, 0}, "Ada"}, {{:names, 1}, "Jay"}] = result

    result =
      CubDB.select(db,
        min_key: :a,
        max_key: :c,
        reverse: true
      )
      |> Enum.into([])

    assert [c: 3, b: 2, a: 1] = result

    result =
      CubDB.select(db,
        min_key: :a,
        max_key: :c,
        reverse: true
      )
      |> Stream.map(fn {_key, value} -> value end)
      |> Stream.take(2)
      |> Enum.sum()

    assert 5 = result
  end

  test "select/2 releases the snapshot after consuming the stream", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir, auto_compact: false)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3)

    CubDB.subscribe(db)

    CubDB.select(db)
    |> Stream.map(fn
      {:a, 1} ->
        :ok = CubDB.compact(db)

      {:b, 2} ->
        assert_receive :compaction_started, 1000
        refute_receive :clean_up_started, 1000

      _ ->
        nil
    end)
    |> Stream.run()

    assert_receive :clean_up_started, 1000
  end

  test "select/2 releases the snapshot in case of a crash", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir, auto_compact: false)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4)

    CubDB.subscribe(db)

    stream =
      CubDB.select(db)
      |> Stream.map(fn
        {:a, 1} ->
          :ok = CubDB.compact(db)

        {:b, 2} ->
          assert_receive :compaction_started, 1000

        {:c, 3} ->
          raise "boom!"
      end)

    assert_raise RuntimeError, "boom!", fn -> Stream.run(stream) end

    assert_receive :clean_up_started, 1000
  end

  test "select/2 raises if the legacy options :pipe or :reduce are passed", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    assert_raise RuntimeError,
                 "select/2 does not have a :reduce option anymore. Use Enum.reduce on the returned lazy stream instead.",
                 fn ->
                   CubDB.select(db, reduce: fn a, b -> a + b end) |> Enum.to_list()
                 end

    assert_raise RuntimeError,
                 "select/2 does not have a :pipe option anymore. Pipe the returned lazy stream into functions in Stream or Enum instead.",
                 fn ->
                   CubDB.select(db, pipe: [map: fn {_, v} -> v end]) |> Enum.to_list()
                 end
  end

  describe "snapshot/2" do
    test "returns a snapshot that cannot be used after release", %{tmp_dir: tmp_dir} do
      {:ok, db} = CubDB.start_link(tmp_dir)

      snap = CubDB.snapshot(db)
      CubDB.release_snapshot(snap)

      assert_raise RuntimeError,
                   "Attempt to use CubDB snapshot after it was released or it timed out",
                   fn ->
                     CubDB.Snapshot.get(snap, :a)
                   end

      assert_raise RuntimeError,
                   "Attempt to use CubDB snapshot after it was released or it timed out",
                   fn ->
                     CubDB.Snapshot.get_multi(snap, [:a, :b])
                   end

      assert_raise RuntimeError,
                   "Attempt to use CubDB snapshot after it was released or it timed out",
                   fn ->
                     CubDB.Snapshot.fetch(snap, :a)
                   end

      assert_raise RuntimeError,
                   "Attempt to use CubDB snapshot after it was released or it timed out",
                   fn ->
                     CubDB.Snapshot.has_key?(snap, :a)
                   end

      assert_raise RuntimeError,
                   "Attempt to use CubDB snapshot after it was released or it timed out",
                   fn ->
                     CubDB.Snapshot.size(snap)
                   end
    end

    test "blocks clean up until the snapshot released", %{tmp_dir: tmp_dir} do
      {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
      snap = CubDB.snapshot(db, :infinity)

      CubDB.subscribe(db)

      :ok = CubDB.compact(db)

      assert_receive :compaction_started
      assert_receive :compaction_completed, 1000
      assert_receive :catch_up_completed, 1000
      refute_receive :clean_up_started, 1000

      :ok = CubDB.release_snapshot(snap)

      assert_receive :clean_up_started, 1000
    end

    test "blocks clean up until the snapshot times out", %{tmp_dir: tmp_dir} do
      {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

      CubDB.subscribe(db)

      timeout = 1000
      CubDB.snapshot(db, timeout)
      :ok = CubDB.compact(db)

      assert_receive :compaction_started
      refute_receive :clean_up_started, timeout - 200
      assert_receive :clean_up_started, timeout
    end

    test "releasing a snapshot twice does not error", %{tmp_dir: tmp_dir} do
      {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
      snap = CubDB.snapshot(db)

      assert :ok = CubDB.release_snapshot(snap)
      assert :ok = CubDB.release_snapshot(snap)
    end
  end

  test "with_snapshot/2 automatically releases the snapshot", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    CubDB.put(db, :a, 123)

    CubDB.subscribe(db)

    assert 123 =
             CubDB.with_snapshot(db, fn snap ->
               CubDB.put(db, :a, 0)
               CubDB.Snapshot.get(snap, :a)
             end)

    :ok = CubDB.compact(db)

    assert_receive :compaction_started
    assert_receive :compaction_completed, 1000
    assert_receive :catch_up_completed, 1000
    assert_receive :clean_up_started, 1000
  end

  test "with_snapshot/2 automatically releases the snapshot even in case of an exception", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    CubDB.subscribe(db)

    assert_raise RuntimeError, "boom!", fn ->
      CubDB.with_snapshot(db, fn _ ->
        raise "boom!"
      end)
    end

    :ok = CubDB.compact(db)

    assert_receive :compaction_started
    assert_receive :compaction_completed, 1000
    assert_receive :catch_up_completed, 1000
    assert_receive :clean_up_started, 1000
  end

  test "transaction/2 when fun returns {:commit, :tx, result} commits the transaction and returns the result",
       %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
    :ok = CubDB.put_multi(db, a: 0, b: 0, x: 0)

    assert 123 =
             CubDB.transaction(db, fn tx ->
               tx = CubDB.Tx.put(tx, :a, 1)
               tx = CubDB.Tx.put(tx, :b, 2)
               tx = CubDB.Tx.delete(tx, :x)
               {:commit, tx, 123}
             end)

    assert 1 = CubDB.get(db, :a)
    assert 2 = CubDB.get(db, :b)
    assert :error = CubDB.fetch(db, :x)

    :ok = CubDB.stop(db)
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    assert 1 = CubDB.get(db, :a)
    assert 2 = CubDB.get(db, :b)
    assert :error = CubDB.fetch(db, :x)
  end

  test "transaction/2 when fun returns {:cancel, result} cancels the transaction and returns the result",
       %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
    :ok = CubDB.put_multi(db, a: 0, x: 0)

    assert 123 =
             CubDB.transaction(db, fn tx ->
               tx = CubDB.Tx.put(tx, :a, 1)
               CubDB.Tx.delete(tx, :x)
               {:cancel, 123}
             end)

    assert 0 = CubDB.get(db, :a)
    assert 0 = CubDB.get(db, :x)

    :ok = CubDB.stop(db)
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    assert 0 = CubDB.get(db, :a)
    assert 0 = CubDB.get(db, :x)
  end

  test "transaction/2 when fun raises an exception cancels the transaction and re-raises the exception",
       %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
    :ok = CubDB.put_multi(db, a: 0, x: 0)

    assert_raise RuntimeError, "boom!", fn ->
      CubDB.transaction(db, fn tx ->
        tx = CubDB.Tx.put(tx, :a, 1)
        CubDB.Tx.delete(tx, :x)
        raise "boom!"
      end)
    end

    :ok = CubDB.stop(db)
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    assert 0 = CubDB.get(db, :a)
    assert 0 = CubDB.get(db, :x)
  end

  test "transaction/2 when fun throws a value cancels the transaction and re-throws the value", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
    :ok = CubDB.put_multi(db, a: 0, x: 0)

    assert :hello =
             catch_throw(
               CubDB.transaction(db, fn tx ->
                 tx = CubDB.Tx.put(tx, :a, 1)
                 CubDB.Tx.delete(tx, :x)
                 throw(:hello)
               end)
             )

    :ok = CubDB.stop(db)
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    assert 0 = CubDB.get(db, :a)
    assert 0 = CubDB.get(db, :x)
  end

  test "transaction/2 when fun exits cancels the transaction and exits again with the same value",
       %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
    :ok = CubDB.put_multi(db, a: 0, x: 0)

    assert "hello" =
             catch_exit(
               CubDB.transaction(db, fn tx ->
                 tx = CubDB.Tx.put(tx, :a, 1)
                 CubDB.Tx.delete(tx, :x)
                 exit("hello")
               end)
             )

    :ok = CubDB.stop(db)
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    assert 0 = CubDB.get(db, :a)
    assert 0 = CubDB.get(db, :x)
  end

  test "transaction/2 raises when used inside another transaction", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    assert_raise RuntimeError,
                 "Cannot start nested write transaction. You might be using CubDB instead of CubDB.Tx to perform writes inside of a transaction",
                 fn ->
                   CubDB.transaction(db, fn _tx ->
                     CubDB.transaction(db, fn _tx -> nil end)
                   end)
                 end
  end

  test "transaction/2 raises when trying to commit another transaction", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    tx =
      CubDB.transaction(db, fn tx ->
        tx = CubDB.Tx.put(tx, :a, 1)
        {:cancel, tx}
      end)

    assert_raise RuntimeError,
                 "Attempt to commit a transaction started by a different owner",
                 fn ->
                   CubDB.transaction(db, fn _ ->
                     {:commit, tx, nil}
                   end)
                 end

    assert CubDB.has_key?(db, :a) == false
  end

  test "reads are concurrent", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    entries = [a: 1, b: 2, c: 3, d: 4]

    CubDB.put_multi(db, entries)

    reads =
      Task.async_stream([:a, :b, :c], fn key ->
        CubDB.get(db, key)
      end)
      |> Enum.to_list()

    assert [ok: 1, ok: 2, ok: 3] = reads
    assert [a: 1, b: 2, c: 3, d: 4] = CubDB.select(db) |> Enum.to_list()
  end

  test "get_and_update_multi/3, get_and_update/3 and update/3 work as expected", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir)

    entries = [a: 1, b: 2, c: 3, d: 4]

    for {key, value} <- entries, do: CubDB.put(db, key, value)

    assert [2, 2] =
             CubDB.get_and_update_multi(db, [:a, :c], fn %{a: a, c: c} ->
               a = a + 1
               c = c - 1
               {[a, c], %{a: a, c: c}, [:d]}
             end)

    assert CubDB.get(db, :a) == 2
    assert CubDB.get(db, :c) == 2
    assert CubDB.has_key?(db, :d) == false

    assert 2 =
             CubDB.get_and_update(db, :b, fn b ->
               {b, b + 3}
             end)

    assert CubDB.get(db, :a) == 2

    assert 5 =
             CubDB.get_and_update(db, :b, fn _ ->
               :pop
             end)

    assert CubDB.has_key?(db, :b) == false

    assert :ok =
             CubDB.update(db, :b, 0, fn b ->
               b + 1
             end)

    assert CubDB.get(db, :b) == 0

    assert :ok =
             CubDB.update(db, :b, 0, fn b ->
               b + 1
             end)

    assert CubDB.get(db, :b) == 1
  end

  test "get_and_update_multi/3 works well during a compaction", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    entries = [a: 1, b: 2, c: 3, d: 4]

    for {key, value} <- entries, do: CubDB.put(db, key, value)

    CubDB.compact(db)

    assert [2, 2] =
             CubDB.get_and_update_multi(db, [:a, :c], fn %{a: a, c: c} ->
               a = a + 1
               c = c - 1
               {[a, c], %{a: a, c: c}, [:d]}
             end)

    assert CubDB.get(db, :a) == 2
    assert CubDB.get(db, :c) == 2
    assert CubDB.has_key?(db, :d) == false
  end

  test "get_multi/3, put_multi/2, delete_multi/2 and put_and_delete_multi/3 work as expected", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir)

    entries = %{a: 1, b: 2, c: 3, d: 4}
    keys = Map.keys(entries)

    assert :ok = CubDB.put_multi(db, entries)
    assert CubDB.size(db) == length(Map.to_list(entries))

    assert entries == CubDB.get_multi(db, keys)
    assert %{c: 3, b: 2} == CubDB.get_multi(db, [:c, :b, :x])

    assert :ok = CubDB.delete_multi(db, keys)
    assert %{} == CubDB.get_multi(db, keys)
    assert CubDB.size(db) == 0

    :ok = CubDB.put_multi(db, %{a: 1, b: 2, c: 3})
    assert :ok = CubDB.put_and_delete_multi(db, %{d: 4, e: 5}, [:a, :c])

    assert [b: 2, d: 4, e: 5] = CubDB.select(db) |> Enum.to_list()
  end

  test "put/3 is persisted to disk", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put(db, :a, 1)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert CubDB.get(db, :a) == 1
  end

  test "put_multi/2 is persisted to disk", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert [a: 1, b: 2, c: 3] = CubDB.select(db) |> Enum.to_list()
  end

  test "update/4 is persisted to disk", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put(db, :a, 1)

    :ok = CubDB.update(db, :a, 0, fn x -> x + 1 end)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert CubDB.get(db, :a) == 2
  end

  test "get_and_update/3 is persisted to disk", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put(db, :a, 1)

    1 = CubDB.get_and_update(db, :a, fn x -> {x, x + 1} end)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert CubDB.get(db, :a) == 2
  end

  test "get_and_update/3 does not perform a write, if the value is unchanged", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put(db, :a, 1)

    state = :sys.get_state(db)

    file_stat =
      CubDB.get_and_update(db, :a, fn x ->
        # Obtain file stat after the get part of get_and_update,
        # otherwise the atime will change
        {:ok, file_stat} = CubDB.current_db_file(db) |> File.stat()
        {file_stat, x}
      end)

    assert {:ok, ^file_stat} = CubDB.current_db_file(db) |> File.stat()
    assert ^state = :sys.get_state(db)
    assert 1 = CubDB.get(db, :a)
  end

  test "concurrent writes are serialized", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    :ok = CubDB.put_multi(db, a: 0, h: 0, v: 0, w: 0, x: 0, y: 0, z: 0)

    tasks =
      CubDB.get_and_update(db, :a, fn _ ->
        task1 = Task.async(fn -> CubDB.put(db, :b, 2) end)
        task2 = Task.async(fn -> CubDB.put_new(db, :c, 3) end)
        task3 = Task.async(fn -> CubDB.delete(db, :v) end)
        task4 = Task.async(fn -> CubDB.put_multi(db, d: 4, e: 5) end)
        task5 = Task.async(fn -> CubDB.delete_multi(db, [:w, :x]) end)
        task6 = Task.async(fn -> CubDB.put_and_delete_multi(db, [f: 6, g: 7], [:y]) end)

        task7 =
          Task.async(fn ->
            CubDB.get_and_update_multi(db, [:h, :i], fn _ ->
              {:ok, %{h: 8, i: 9}, [:z]}
            end)
          end)

        {[task1, task2, task3, task4, task5, task6, task7], 1}
      end)

    results = for task <- tasks, do: Task.await(task)

    assert [:ok, :ok, :ok, :ok, :ok, :ok, :ok] = results

    result = CubDB.select(db) |> Enum.to_list()

    assert result == [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6, g: 7, h: 8, i: 9]
  end

  test "write access is released even if a writer raises", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

    assert_raise RuntimeError, "boom!", fn ->
      CubDB.get_and_update(db, :a, fn _ ->
        raise "boom!"
      end)
    end

    assert :ok = CubDB.put(db, :a, 1)
  end

  test "readers are not blocked by a writer", %{tmp_dir: tmp_dir} do
    test_pid = self()

    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
    :ok = CubDB.put(db, :a, :v0)

    writer_task =
      Task.async(fn ->
        CubDB.get_and_update(db, :a, fn :v0 ->
          send(test_pid, {self(), :writer_task})

          receive do
            :continue -> {:ok, :v1}
          end
        end)
      end)

    assert_receive {writer_pid, :writer_task}
    assert :v0 = CubDB.get(db, :a)
    send(writer_pid, :continue)
    assert :ok = Task.await(writer_task)
    assert :v1 = CubDB.get(db, :a)
  end

  test "readers are not blocked by another reader", %{tmp_dir: tmp_dir} do
    test_pid = self()

    {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
    :ok = CubDB.put(db, :a, :v0)

    reader_task =
      Task.async(fn ->
        CubDB.select(db)
        |> Enum.reduce(nil, fn {:a, :v0}, nil ->
          send(test_pid, {self(), :reader_task})

          receive do
            :continue -> :ok
          end
        end)
      end)

    assert_receive {reader_pid, :reader_task}
    assert :ok = CubDB.put(db, :a, :v1)
    send(reader_pid, :continue)
    assert :ok = Task.await(reader_task)
    assert :v1 = CubDB.get(db, :a)
  end

  test "get_and_update_multi/3 is persisted to disk", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3)

    %{a: 1, b: 2, c: 3} =
      CubDB.get_and_update_multi(db, [:a, :b, :c], fn entries ->
        entries_incremented = entries |> Enum.map(fn {k, v} -> {k, v + 1} end) |> Enum.into(%{})
        {entries, entries_incremented, []}
      end)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert [a: 2, b: 3, c: 4] = CubDB.select(db) |> Enum.to_list()
  end

  test "delete/2 is persisted to disk", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3)

    :ok = CubDB.delete(db, :a)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert CubDB.has_key?(db, :a) == false
  end

  test "delete_multi/2 is persisted to disk", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3)

    :ok = CubDB.delete_multi(db, [:a, :c])

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)
    assert [b: 2] = CubDB.select(db) |> Enum.to_list()
  end

  test "start_link/1 uses the last filename (in base 16)", %{tmp_dir: tmp_dir} do
    File.touch(Path.join(tmp_dir, "F.cub"))
    File.touch(Path.join(tmp_dir, "X.cub"))
    File.touch(Path.join(tmp_dir, "10.cub"))

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert CubDB.current_db_file(db) == Path.join(tmp_dir, "10.cub")
  end

  test "compaction switches to a new file incrementing in base 16", %{tmp_dir: tmp_dir} do
    File.touch(Path.join(tmp_dir, "F.cub"))
    File.touch(Path.join(tmp_dir, "X.cub"))
    File.touch(Path.join(tmp_dir, "10.cub"))

    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    CubDB.subscribe(db)

    :ok = CubDB.compact(db)

    assert_receive :compaction_started
    assert_receive :compaction_completed, 1000
    assert_receive :catch_up_completed, 1000

    assert CubDB.current_db_file(db) == Path.join(tmp_dir, "11.cub")
  end

  test "compaction catches up on newer updates", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    entries = [a: 1, b: 2, c: 3, d: 4, e: 5]

    for {key, value} <- entries, do: CubDB.put(db, key, value)

    assert CubDB.dirt_factor(db) > 0

    CubDB.subscribe(db)

    CubDB.compact(db)
    CubDB.put(db, :x, 0)
    CubDB.delete(db, :a)

    assert_receive :compaction_started
    assert_receive :compaction_completed, 1000
    assert_receive :catch_up_completed, 1000

    assert CubDB.size(db) == 5
    assert CubDB.get(db, :x) == 0
    assert CubDB.has_key?(db, :a) == false

    assert :ok = CubDB.compact(db)
  end

  test "catch up waits for pending writes", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    entries = [a: 1, b: 2, c: 3]

    for {key, value} <- entries, do: CubDB.put(db, key, value)

    CubDB.subscribe(db)

    1 =
      CubDB.get_and_update(db, :a, fn a ->
        CubDB.compact(db)
        assert_receive :compaction_completed, 1000
        Process.sleep(1000)
        {a, 10}
      end)

    assert_receive :catch_up_completed, 1000
    assert CubDB.get(db, :a) == 10
  end

  test "compaction stops the old Btree upon clean-up, releasing resources", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    CubDB.subscribe(db)

    %CubDB.State{btree: old_btree} = :sys.get_state(db)

    :ok = CubDB.compact(db)

    assert_receive :compaction_started
    assert_receive :compaction_completed, 1000
    assert_receive :catch_up_completed, 1000
    assert_receive :clean_up_started, 1000

    assert CubDB.Btree.alive?(old_btree) == false
    assert %CubDB.State{old_btrees: []} = :sys.get_state(db)
  end

  test "compaction does not leak process links", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    CubDB.subscribe(db)

    links_before = Process.info(db)[:links]
    :ok = CubDB.compact(db)

    assert_receive :clean_up_started, 1000

    links_after = Process.info(db)[:links]
    assert length(links_before) == length(links_after)
  end

  test "auto compaction triggers compaction when conditions are met", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: {3, 0.3})

    assert CubDB.dirt_factor(db) == 0

    CubDB.subscribe(db)

    CubDB.put(db, :a, 1)
    refute_received :compaction_started

    CubDB.put(db, :b, 2)
    refute_received :compaction_started

    CubDB.put(db, :a, 3)
    assert_received :compaction_started

    CubDB.put(db, :a, 4)
    refute_received :compaction_started
  end

  test "auto compaction is active by default", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    assert %CubDB.State{auto_compact: {100, 0.25}} = :sys.get_state(db)
  end

  test "set_auto_compact/1 configures auto compaction behavior", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    assert %CubDB.State{auto_compact: false} = :sys.get_state(db)

    :ok = CubDB.set_auto_compact(db, true)

    assert %CubDB.State{auto_compact: {100, 0.25}} = :sys.get_state(db)

    :ok = CubDB.set_auto_compact(db, false)

    assert %CubDB.State{auto_compact: false} = :sys.get_state(db)

    :ok = CubDB.set_auto_compact(db, {10, 0.5})

    assert %CubDB.State{auto_compact: {10, 0.5}} = :sys.get_state(db)

    assert {:error, _} = CubDB.set_auto_compact(db, {:x, 100})
  end

  test "compact/1 performs compaction and catch up", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4, e: 5)
    CubDB.subscribe(db)

    original_file = CubDB.current_db_file(db)

    assert :ok = CubDB.compact(db)
    :ok = CubDB.put(db, :f, 6)

    assert_received :compaction_started
    assert_receive :compaction_completed, 100
    assert_receive :catch_up_completed, 100

    assert CubDB.select(db) |> Enum.to_list() == [a: 1, b: 2, c: 3, d: 4, e: 5, f: 6]
    refute CubDB.current_db_file(db) == original_file
  end

  test "compact/1 returns :ok, or {:error, :pending_compaction} if already compacting", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4, e: 5)
    CubDB.subscribe(db)

    # Get snapshot to prevent compaction to complete
    snap = CubDB.snapshot(db, :infinity)

    assert :ok = CubDB.compact(db)
    assert_received :compaction_started

    assert {:error, :pending_compaction} = CubDB.compact(db)
    refute_received :compaction_started

    # Release snapshot to allow compaction to complete
    CubDB.release_snapshot(snap)

    assert_receive :compaction_completed, 200
    assert_receive :catch_up_completed, 200

    assert :ok = CubDB.compact(db)
  end

  test "compact/1 postpones clean-up when old file is still referenced", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
    :ok = CubDB.put(db, :foo, 123)

    CubDB.subscribe(db)

    caller = self()

    # This blocks the reader until we send a :resume message
    Task.start_link(fn ->
      CubDB.select(db)
      |> Enum.reduce(0, fn _, a ->
        if a == 0 do
          send(caller, {:stopping, self()})

          receive do
            :resume -> nil
          end
        end

        a + 1
      end)
    end)

    reader =
      receive do
        {:stopping, pid} -> pid
      end

    :ok = CubDB.compact(db)

    assert_received :compaction_started
    refute_receive :clean_up_started

    assert %CubDB.State{clean_up_pending: true} = :sys.get_state(db)

    send(reader, :resume)
    assert_receive :clean_up_started
  end

  test "compact/1 does not crash if compaction task crashes", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4, e: 5)

    assert :ok = CubDB.compact(db)

    %{compactor: compactor} = :sys.get_state(db)
    Process.exit(compactor, :kill)

    refute Process.alive?(compactor)

    Process.sleep(100)
    assert Process.alive?(db)
  end

  test "halt_compaction/1 returns {:error, :no_compaction_running} if no compaction is running",
       %{
         tmp_dir: tmp_dir
       } do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)

    assert {:error, :no_compaction_running} = CubDB.halt_compaction(db)
  end

  test "halt_compaction/1 stops running compactions and cleans up", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4, e: 5)

    CubDB.subscribe(db)

    assert :ok = CubDB.compact(db)
    assert :ok = CubDB.halt_compaction(db)
    refute CubDB.compacting?(db)
    assert_receive :clean_up_started
  end

  test "compacting?/1 returns true if a compaction is running, otherwise false", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4, e: 5)
    CubDB.subscribe(db)

    # Get snapshot to prevent compaction to complete
    snap = CubDB.snapshot(db, :infinity)

    refute CubDB.compacting?(db)
    assert :ok = CubDB.compact(db)
    assert CubDB.compacting?(db)

    # Release snapshot to allow compaction to complete
    CubDB.release_snapshot(snap)

    assert_receive :compaction_completed, 200
    assert_receive :catch_up_completed, 200

    refute CubDB.compacting?(db)
  end

  test "auto_file_sync is true by default", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    assert %CubDB.State{auto_file_sync: true} = :sys.get_state(db)
  end

  test "set_auto_file_sync/1 configures auto file sync behavior", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_file_sync: false)

    assert %CubDB.State{auto_file_sync: false} = :sys.get_state(db)

    CubDB.set_auto_file_sync(db, true)

    assert %CubDB.State{auto_file_sync: true} = :sys.get_state(db)

    CubDB.set_auto_file_sync(db, false)

    assert %CubDB.State{auto_file_sync: false} = :sys.get_state(db)
  end

  test "file_sync/1 returns :ok", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    assert :ok = CubDB.file_sync(db)
  end

  describe "back_up/2" do
    setup do
      {backup_dir, 0} = System.cmd("mktemp", ["-d"])
      backup_dir = String.trim(backup_dir)

      on_exit(fn ->
        File.rm_rf!(backup_dir)
      end)

      {:ok, backup_dir: backup_dir}
    end

    test "returns error tuple if the target directory already exists", %{
      tmp_dir: tmp_dir,
      backup_dir: backup_dir
    } do
      {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
      assert {:error, :eexist} = CubDB.back_up(db, backup_dir)
    end

    test "creates a backup of the current state of the database", %{
      tmp_dir: tmp_dir,
      backup_dir: backup_dir
    } do
      File.rm_rf!(backup_dir)
      {:ok, db} = CubDB.start_link(data_dir: tmp_dir)
      :ok = CubDB.put_multi(db, foo: 1, bar: 2, baz: 3)

      assert :ok = CubDB.back_up(db, backup_dir)

      {:ok, copy} = CubDB.start_link(data_dir: backup_dir)

      assert CubDB.select(db) |> Enum.to_list() == CubDB.select(copy) |> Enum.to_list()
    end
  end

  test "data_dir/1 returns the path to the data directory", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    tmp_dir_string = to_string(tmp_dir)
    assert ^tmp_dir_string = CubDB.data_dir(db)
  end

  test "current_db_file/1 returns the path to the current database file", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
    expected_file_path = Path.join(tmp_dir, "0.cub")
    assert ^expected_file_path = CubDB.current_db_file(db)

    CubDB.subscribe(db)
    CubDB.compact(db)

    assert_receive :catch_up_completed
    assert_receive :clean_up_started

    expected_file_path = Path.join(tmp_dir, "1.cub")
    assert ^expected_file_path = CubDB.current_db_file(db)
  end

  test "cubdb_file?/1 returns false for non-cubdb named files" do
    bad_filenames = [
      "",
      "./db/5432 (copy).cub",
      "1234",
      "/opt/data/db/11111.cubb"
    ]

    for filename <- bad_filenames do
      refute CubDB.cubdb_file?(filename)
    end
  end

  test "cubdb_file?/1 returns true for cubdb named files" do
    good_filenames = [
      "0.cub",
      "0.compact",
      "./db/5432.cub",
      "1234.compact",
      "/opt/data/db/11111.cub"
    ]

    for filename <- good_filenames do
      assert CubDB.cubdb_file?(filename)
    end
  end

  test "bad filenames don't cause a crash", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: false)
    Path.join(tmp_dir, "blah.cub") |> File.touch()

    CubDB.subscribe(db)
    CubDB.compact(db)
    assert_receive :compaction_completed
    assert Process.alive?(db)
  end

  test "clear/1 deletes all entries", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3)

    :ok = CubDB.clear(db)

    assert CubDB.size(db) == 0

    for key <- [:a, :b, :c] do
      assert CubDB.has_key?(db, key) == false
    end
  end

  test "clear/1 is persisted", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3)

    :ok = CubDB.clear(db)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert CubDB.size(db) == 0

    for key <- [:a, :b, :c] do
      assert CubDB.has_key?(db, key) == false
    end
  end

  test "clear/1 behaves well during compaction", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.subscribe(db)

    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4, e: 5)
    :ok = CubDB.compact(db)

    assert_received :compaction_started

    :ok = CubDB.clear(db)

    assert_receive :compaction_completed, 1000
    assert_receive :catch_up_completed, 1000

    assert CubDB.size(db) == 0

    for key <- [:a, :b, :c] do
      assert CubDB.has_key?(db, key) == false
    end
  end
end
