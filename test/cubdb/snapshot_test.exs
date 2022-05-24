defmodule CubDB.SnapshotTest do
  use ExUnit.Case

  setup do
    {tmp_dir, 0} = System.cmd("mktemp", ["-d"])
    tmp_dir = tmp_dir |> String.trim()

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "get, get_multi, fetch, has_key?, size, select, and select_stream work as expected", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.put_multi(db, a: 1, c: 3)

    snap = CubDB.snapshot(db)
    :ok = CubDB.put_multi(db, a: 2, b: 3)

    assert 1 = CubDB.Snapshot.get(snap, :a)
    assert 0 = CubDB.Snapshot.get(snap, :b, 0)

    assert %{:a => 1, :c => 3} = CubDB.Snapshot.get_multi(snap, [:a, :b, :c])

    assert {:ok, 1} = CubDB.Snapshot.fetch(snap, :a)
    assert :error = CubDB.Snapshot.fetch(snap, :b)

    assert CubDB.Snapshot.has_key?(snap, :a)
    refute CubDB.Snapshot.has_key?(snap, :b)

    assert [a: 1, c: 3] = CubDB.Snapshot.select(snap)

    assert [a: 1, c: 3] = CubDB.Snapshot.select_stream(snap) |> Enum.into([])

    assert 2 = CubDB.Snapshot.size(snap)

    assert 2 = CubDB.get(db, :a)
    assert 3 = CubDB.get(db, :b)
    assert 3 = CubDB.size(db)
  end

  test "read operations extend the validity of the snapshot until the end of the read operation",
       %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.subscribe(db)

    CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4, e: 5)

    snap = CubDB.snapshot(db, 50)
    :ok = CubDB.compact(db)

    result =
      CubDB.Snapshot.select(snap,
        pipe: [
          map: fn x ->
            Process.sleep(20)
            x
          end
        ]
      )

    assert result == [a: 1, b: 2, c: 3, d: 4, e: 5]
    assert_receive :clean_up_started, 1000

    snap = CubDB.snapshot(db, 50)
    :ok = CubDB.compact(db)

    result =
      CubDB.Snapshot.select_stream(snap)
      |> Stream.map(fn x ->
        Process.sleep(20)
        x
      end)
      |> Enum.into([])

    assert result == [a: 1, b: 2, c: 3, d: 4, e: 5]
    assert_receive :clean_up_started, 1000
  end

  test "select_stream/2 raises if the stream is consumed when the snapshot is not valid anymore",
       %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    CubDB.put(db, :a, 1)

    stream =
      CubDB.with_snapshot(db, fn snap ->
        CubDB.Snapshot.select_stream(snap)
      end)

    assert_raise RuntimeError,
                 "Attempt to use CubDB snapshot after it was released or it timed out",
                 fn ->
                   Stream.run(stream)
                 end
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
      snap = CubDB.snapshot(db)
      assert {:error, :eexist} = CubDB.Snapshot.back_up(snap, backup_dir)
    end

    test "creates a backup of the given snapshot", %{tmp_dir: tmp_dir, backup_dir: backup_dir} do
      File.rm_rf!(backup_dir)
      {:ok, db} = CubDB.start_link(data_dir: tmp_dir)

      :ok = CubDB.put_multi(db, foo: 1, bar: 2, baz: 3)

      CubDB.with_snapshot(db, fn snap ->
        :ok = CubDB.put_multi(db, foo: 0, qux: 4)

        assert :ok = CubDB.Snapshot.back_up(snap, backup_dir)

        {:ok, copy} = CubDB.start_link(data_dir: backup_dir)

        assert CubDB.Snapshot.select(snap) == CubDB.select(copy)
      end)
    end
  end
end
