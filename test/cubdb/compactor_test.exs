defmodule CubDB.CompactorTest do
  use ExUnit.Case

  alias CubDB.Btree
  alias CubDB.Compactor
  alias CubDB.Store

  describe "end-to-end" do
    setup do
      {tmp_dir, 0} = System.cmd("mktemp", ["-d"])
      tmp_dir = tmp_dir |> String.trim()

      on_exit(fn ->
        File.rm_rf!(tmp_dir)
      end)

      {:ok, tmp_dir: tmp_dir}
    end

    test "run/3 compacts and catch up a Btree into the given Store", %{tmp_dir: tmp_dir} do
      {:ok, db} = CubDB.start_link(data_dir: tmp_dir, auto_compaction: false)

      entries = [foo: 1, bar: 2, baz: 3]
      CubDB.put_multi(db, entries)

      more_entries = [qux: 4, quux: 5]

      {:ok, store} = Store.File.create(Path.join(tmp_dir, "1.compact"))

      CubDB.subscribe(db)

      CubDB.get_and_update_multi(db, [], fn _ ->
        Task.async(Compactor, :run, [db, store])
        {:ok, more_entries, []}
      end)

      assert_receive :compaction_completed, 1000
      assert_receive :catch_up_completed, 1000

      all_entries = Enum.concat(entries, more_entries) |> Enum.sort()

      assert {:ok, ^all_entries} = CubDB.select(db)
      assert CubDB.current_db_file(db) == Path.join(tmp_dir, "1.cub")
      refute Store.open?(store)
    end
  end

  test "compact/2 compacts a Btree into the given store and returns a new compacted Btree" do
    {:ok, store} = Store.TestStore.create()
    {:ok, target_store} = Store.TestStore.create()

    btree =
      Enum.reduce([foo: 1, bar: 2, baz: 3], Btree.new(store), fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    compacted_btree = Compactor.compact(btree, target_store)

    assert Enum.to_list(compacted_btree) == Enum.to_list(btree)
    assert Btree.dirt_factor(btree) > 0
    assert Btree.dirt_factor(compacted_btree) == 0
    assert compacted_btree.store == target_store
  end

  test "catch_up_iter/3 catches up the Diff between the second and the third btree on top of the first btree" do
    {:ok, store} = Store.TestStore.create()
    {:ok, target_store} = Store.TestStore.create()

    original_btree =
      Enum.reduce([foo: 1, bar: 2, baz: 3], Btree.new(store), fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    latest_btree =
      Enum.reduce([abc: 123, qux: 4], original_btree, fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    target_btree =
      Btree.new(target_store)
      |> Btree.insert(:a, 321)

    btree = Compactor.catch_up_iter(target_btree, original_btree, latest_btree)

    assert Enum.to_list(btree) == [a: 321, abc: 123, qux: 4]
    assert btree.store == target_store
  end
end
