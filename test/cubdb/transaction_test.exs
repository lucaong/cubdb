defmodule CubDB.TransactionTest do
  use ExUnit.Case

  setup do
    {tmp_dir, 0} = System.cmd("mktemp", ["-d"])
    tmp_dir = tmp_dir |> String.trim()

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "get, get_multi, fetch, has_key?, size, select work as expected", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.put_multi(db, a: 1, c: 3)

    CubDB.transaction(db, fn tx ->
      tx = CubDB.Tx.put(tx, :b, 2)
      tx = CubDB.Tx.delete(tx, :c)

      assert 1 = CubDB.Tx.get(tx, :a)
      assert 2 = CubDB.Tx.get(tx, :b)
      assert 0 = CubDB.Tx.get(tx, :c, 0)

      assert {:ok, 1} = CubDB.Tx.fetch(tx, :a)
      assert {:ok, 2} = CubDB.Tx.fetch(tx, :b)
      assert :error = CubDB.Tx.fetch(tx, :c)

      assert CubDB.Tx.has_key?(tx, :a)
      assert CubDB.Tx.has_key?(tx, :b)
      refute CubDB.Tx.has_key?(tx, :c)

      assert 2 = CubDB.Tx.size(tx)

      assert [a: 1, b: 2] = CubDB.Tx.select(tx) |> Enum.into([])

      {:cancel, nil}
    end)

    assert [a: 1, c: 3] = CubDB.select(db) |> Enum.into([])
  end

  test "get_metadata, delete_metadata, put_metadata work as expected", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.put_multi(db, a: 1, c: 3)
    CubDB.put_metadata(db, :foo, 123)

    CubDB.transaction(db, fn tx ->
      tx = CubDB.Tx.put_metadata(tx, :foo, 456)
      tx = CubDB.Tx.put_metadata(tx, :bar, 789)

      assert 456 == CubDB.Tx.get_metadata(tx, :foo)
      assert 789 == CubDB.Tx.get_metadata(tx, :bar)

      tx = CubDB.Tx.delete_metadata(tx, :bar)

      assert nil == CubDB.Tx.get_metadata(tx, :bar)

      {:cancel, nil}
    end)

    assert 123 == CubDB.get_metadata(db, :foo)
    assert [a: 1, c: 3] = CubDB.select(db) |> Enum.into([])
  end

  test "refetch/3 returns :unchanged if the entry was not written, otherwise fetches it", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir, auto_compact: false)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4)

    CubDB.with_snapshot(db, fn snap ->
      :ok = CubDB.put_multi(db, b: 0, c: 3)
      :ok = CubDB.delete(db, :d)

      CubDB.transaction(db, fn tx ->
        assert :unchanged = CubDB.Tx.refetch(tx, :a, snap)
        assert {:ok, 0} = CubDB.Tx.refetch(tx, :b, snap)
        assert {:ok, 3} = CubDB.Tx.refetch(tx, :c, snap)
        assert :error = CubDB.Tx.refetch(tx, :d, snap)
        assert :error = CubDB.Tx.refetch(tx, :e, snap)

        {:cancel, nil}
      end)
    end)
  end

  test "refetch/2 raises an exception if called with a snapshot from another process", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(data_dir: tmp_dir, auto_compact: false)
    snap = CubDB.snapshot(db, :infinity)
    CubDB.stop(db)

    {:ok, db} = CubDB.start_link(data_dir: tmp_dir, auto_compact: false)

    assert_raise RuntimeError, "Invalid snapshot from another CubDB process", fn ->
      CubDB.transaction(db, fn tx ->
        CubDB.Tx.refetch(tx, :a, snap)

        {:cancel, nil}
      end)
    end
  end

  test "put/3 inserts an entry if committed", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.put(db, :a, 0)

    CubDB.transaction(db, fn tx ->
      tx = CubDB.Tx.put(tx, :a, 1)
      tx = CubDB.Tx.put(tx, :b, 2)
      assert 1 = CubDB.Tx.get(tx, :a)
      assert 2 = CubDB.Tx.get(tx, :b)
      {:cancel, nil}
    end)

    assert CubDB.get_multi(db, [:a, :b]) == %{a: 0}

    CubDB.transaction(db, fn tx ->
      tx = CubDB.Tx.put(tx, :a, 1)
      tx = CubDB.Tx.put(tx, :b, 2)
      assert 1 = CubDB.Tx.get(tx, :a)
      assert 2 = CubDB.Tx.get(tx, :b)
      {:commit, tx, nil}
    end)

    assert CubDB.get_multi(db, [:a, :b]) == %{a: 1, b: 2}
  end

  test "put_new/3 inserts a new entry if committed", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.put(db, :a, 0)

    CubDB.transaction(db, fn tx ->
      {:error, :exists} = CubDB.Tx.put_new(tx, :a, 1)
      tx = CubDB.Tx.put_new(tx, :b, 2)
      assert 2 = CubDB.Tx.get(tx, :b)
      {:cancel, nil}
    end)

    assert CubDB.get_multi(db, [:a, :b]) == %{a: 0}

    CubDB.transaction(db, fn tx ->
      {:error, :exists} = CubDB.Tx.put_new(tx, :a, 1)
      tx = CubDB.Tx.put_new(tx, :b, 2)
      assert 2 = CubDB.Tx.get(tx, :b)
      {:commit, tx, nil}
    end)

    assert CubDB.get_multi(db, [:a, :b]) == %{a: 0, b: 2}
  end

  test "delete/2 removes an entry if committed", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.put(db, :a, 0)

    CubDB.transaction(db, fn tx ->
      tx = CubDB.Tx.delete(tx, :a)
      refute CubDB.Tx.has_key?(tx, :a)
      {:cancel, nil}
    end)

    assert CubDB.has_key?(db, :a)

    CubDB.transaction(db, fn tx ->
      tx = CubDB.Tx.delete(tx, :a)
      refute CubDB.Tx.has_key?(tx, :a)
      {:commit, tx, nil}
    end)

    refute CubDB.has_key?(db, :a)
  end

  test "clear/1 removes all entries if committed", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.put_multi(db, a: 1, b: 2)

    CubDB.transaction(db, fn tx ->
      tx = CubDB.Tx.clear(tx)
      assert 0 = CubDB.Tx.size(tx)
      {:cancel, nil}
    end)

    assert CubDB.has_key?(db, :a)
    assert CubDB.has_key?(db, :b)
    assert 2 = CubDB.size(db)

    CubDB.transaction(db, fn tx ->
      tx = CubDB.Tx.clear(tx)
      assert 0 = CubDB.Tx.size(tx)
      {:commit, tx, nil}
    end)

    refute CubDB.has_key?(db, :a)
    refute CubDB.has_key?(db, :b)
    assert 0 = CubDB.size(db)
  end

  test "using the transaction outside of its scope raises an exception", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    tx =
      CubDB.transaction(db, fn tx ->
        {:cancel, tx}
      end)

    assert_raise RuntimeError,
                 "Invalid transaction, likely because it was used outside of its scope",
                 fn ->
                   CubDB.Tx.get(tx, :a)
                 end

    assert_raise RuntimeError,
                 "Invalid transaction, likely because it was used outside of its scope",
                 fn ->
                   CubDB.Tx.fetch(tx, :a)
                 end

    assert_raise RuntimeError,
                 "Invalid transaction, likely because it was used outside of its scope",
                 fn ->
                   CubDB.Tx.size(tx)
                 end

    assert_raise RuntimeError,
                 "Invalid transaction, likely because it was used outside of its scope",
                 fn ->
                   CubDB.Tx.select(tx) |> Enum.into([])
                 end

    assert_raise RuntimeError,
                 "Invalid transaction, likely because it was used outside of its scope",
                 fn ->
                   CubDB.Tx.put(tx, :a, 1)
                 end

    assert_raise RuntimeError,
                 "Invalid transaction, likely because it was used outside of its scope",
                 fn ->
                   CubDB.Tx.delete(tx, :a)
                 end

    assert_raise RuntimeError,
                 "Invalid transaction, likely because it was used outside of its scope",
                 fn ->
                   CubDB.Tx.clear(tx)
                 end
  end
end
