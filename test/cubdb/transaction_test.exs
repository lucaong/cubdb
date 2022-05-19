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

  test "get, get_multi, fetch, has_key?, size, and select work as expected", %{tmp_dir: tmp_dir} do
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

      assert {:ok, [a: 1, b: 2]} = CubDB.Tx.select(tx)

      {:cancel, nil}
    end)

    assert {:ok, [a: 1, c: 3]} = CubDB.select(db)
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
end
