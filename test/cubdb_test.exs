defmodule CubDBTest do
  use ExUnit.Case, async: true
  doctest CubDB

  setup do
    tmp_dir = :os.cmd('mktemp -d') |> List.to_string() |> String.trim() |> String.to_charlist()

    on_exit(fn ->
      with {:ok, files} <- File.ls(tmp_dir) do
        for file <- files, do: File.rm(Path.join(tmp_dir, file))
      end

      :ok = File.rmdir(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "start_link/3 starts and links the process", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    assert Process.alive?(db) == true

    {:links, links} = Process.info(self(), :links)
    assert Enum.member?(links, db) == true
  end

  test "start_link/3 returns error if options are invalid", %{tmp_dir: tmp_dir} do
    assert {:error, _} = CubDB.start_link(tmp_dir, auto_compact: "maybe")
  end

  test "start/3 starts the process without linking", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start(tmp_dir)
    assert Process.alive?(db) == true

    {:links, links} = Process.info(self(), :links)
    assert Enum.member?(links, db) == false
  end

  test "child_spec/1 accepts data_dir as a string or charlist argument" do
    string_data_dir = "some_data_dir"
    charlist_data_dir = 'some_data_dir'

    assert %{
             id: CubDB,
             start: {CubDB, :start_link, [^string_data_dir]}
           } = CubDB.child_spec(string_data_dir)

    assert %{
             id: CubDB,
             start: {CubDB, :start_link, [^string_data_dir]}
           } = CubDB.child_spec(charlist_data_dir)
  end

  test "child_spec/1 accepts data_dir and other options as a keyword list" do
    data_dir = "some_data_dir"

    assert %{
             id: CubDB,
             start: {CubDB, :start_link, [^data_dir, [foo: 123], []]}
           } = CubDB.child_spec(data_dir: data_dir, foo: 123)

    options = [auto_compact: true, auto_file_sync: false]

    gen_server_opts = [
      name: :foo,
      timeout: 5000,
      spawn_opt: :something,
      hibernate_after: 3000,
      debug: :bar
    ]

    arg = options |> Keyword.merge(gen_server_opts) |> Keyword.merge(data_dir: data_dir)

    assert %{
             id: CubDB,
             start: {CubDB, :start_link, [^data_dir, ^options, ^gen_server_opts]}
           } = CubDB.child_spec(arg)
  end

  test "child_spec/1 raises a helpful error if data_dir is not given" do
    message =
      "no data_dir given. CubDB.child_spec/1 must be given a keyword list including a :data_dir."

    assert_raise ArgumentError, message, fn ->
      CubDB.child_spec(foo: 123)
    end
  end

  test "put/3, get/3, fetch/2, delete/3, and has_key?/2 work as expected", %{tmp_dir: tmp_dir} do
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
  end

  test "select/3 works as expected", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    entries = [
      {{:names, 0}, "Ada"},
      {{:names, 2}, "Zoe"},
      {{:names, 1}, "Jay"},
      {:a, 1},
      {:b, 2},
      {:c, 3}
    ]

    for {key, value} <- entries, do: CubDB.put(db, key, value)

    assert {:ok, result} =
             CubDB.select(db,
               min_key: {:names, 0},
               max_key: {:names, 2},
               max_key_inclusive: false
             )

    assert result == [{{:names, 0}, "Ada"}, {{:names, 1}, "Jay"}]

    assert {:ok, result} =
             CubDB.select(db,
               min_key: :a,
               max_key: :c,
               pipe: [
                 map: fn {_, value} -> value end
               ],
               reduce: fn n, sum -> sum + n end
             )

    assert result == 6
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
    assert {:ok, [a: 1, b: 2, c: 3, d: 4]} = CubDB.select(db)
  end

  test "get_and_update_multi/4, get_and_update/3 and update/3 work as expected", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir)

    entries = [a: 1, b: 2, c: 3, d: 4]

    for {key, value} <- entries, do: CubDB.put(db, key, value)

    assert {:ok, result} =
             CubDB.get_and_update_multi(db, [:a, :c], fn %{a: a, c: c} ->
               a = a + 1
               c = c - 1
               {[a, c], %{a: a, c: c}, [:d]}
             end)

    assert result == [2, 2]
    assert CubDB.get(db, :a) == 2
    assert CubDB.get(db, :c) == 2
    assert CubDB.has_key?(db, :d) == false

    assert {:ok, result} =
             CubDB.get_and_update(db, :b, fn b ->
               {b, b + 3}
             end)

    assert result == 2
    assert CubDB.get(db, :b) == 5

    assert {:ok, result} =
             CubDB.get_and_update(db, :b, fn _ ->
               :pop
             end)

    assert result == 5
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

    assert {:error, error} =
             CubDB.get_and_update_multi(db, [:a, :c], fn _ ->
               raise(RuntimeError, message: "boom")
             end)

    assert %RuntimeError{message: "boom"} = error
  end

  test "get_and_update_multi/4 works well during a compaction", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    entries = [a: 1, b: 2, c: 3, d: 4]

    for {key, value} <- entries, do: CubDB.put(db, key, value)

    CubDB.compact(db)

    assert {:ok, result} =
             CubDB.get_and_update_multi(db, [:a, :c], fn %{a: a, c: c} ->
               a = a + 1
               c = c - 1
               {[a, c], %{a: a, c: c}, [:d]}
             end)

    assert result == [2, 2]
    assert CubDB.get(db, :a) == 2
    assert CubDB.get(db, :c) == 2
    assert CubDB.has_key?(db, :d) == false
  end

  test "get_multi/3, put_multi/2 and delete_multi/2 work as expected", %{tmp_dir: tmp_dir} do
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

    assert {:ok, [a: 1, b: 2, c: 3]} = CubDB.select(db)
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

    {:ok, 1} = CubDB.get_and_update(db, :a, fn x -> {x, x + 1} end)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert CubDB.get(db, :a) == 2
  end

  test "get_and_update_multi/4 is persisted to disk", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3)

    {:ok, %{a: 1, b: 2, c: 3}} =
      CubDB.get_and_update_multi(db, [:a, :b, :c], fn entries ->
        entries_incremented = entries |> Enum.map(fn {k, v} -> {k, v + 1} end) |> Enum.into(%{})
        {entries, entries_incremented, []}
      end)

    GenServer.stop(db)

    {:ok, db} = CubDB.start_link(tmp_dir)

    assert {:ok, [a: 2, b: 3, c: 4]} = CubDB.select(db)
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
    assert {:ok, [b: 2]} = CubDB.select(db)
  end

  test "start_link/3 uses the last filename (in base 16)", %{tmp_dir: tmp_dir} do
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

    {:ok, db} = CubDB.start_link(tmp_dir)

    CubDB.subscribe(db)

    :ok = CubDB.compact(db)

    assert_receive :compaction_started
    assert_receive :compaction_completed, 1000
    assert_receive :catch_up_completed, 1000

    assert CubDB.current_db_file(db) == Path.join(tmp_dir, "11.cub")
  end

  test "compaction catches up on newer updates", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

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

  test "set_auto_compact/1 configures auto compaction behavior", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_compact: true)

    assert %CubDB.State{auto_compact: {100, 0.25}} = :sys.get_state(db)

    :ok = CubDB.set_auto_compact(db, false)

    assert %CubDB.State{auto_compact: false} = :sys.get_state(db)

    :ok = CubDB.set_auto_compact(db, true)

    assert %CubDB.State{auto_compact: {100, 0.25}} = :sys.get_state(db)

    :ok = CubDB.set_auto_compact(db, {10, 0.5})

    assert %CubDB.State{auto_compact: {10, 0.5}} = :sys.get_state(db)

    assert {:error, _} = CubDB.set_auto_compact(db, {:x, 100})
  end

  test "compact/1 returns :ok, or {:error, :pending_compaction} if already compacting", %{
    tmp_dir: tmp_dir
  } do
    {:ok, db} = CubDB.start_link(tmp_dir)
    :ok = CubDB.put_multi(db, a: 1, b: 2, c: 3, d: 4, e: 5)
    CubDB.subscribe(db)

    assert :ok = CubDB.compact(db)
    assert_received :compaction_started

    assert {:error, :pending_compaction} = CubDB.compact(db)
    refute_received :compaction_started
  end

  test "compact/1 postpones clean-up when old file is still referenced", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)

    CubDB.subscribe(db)

    %CubDB.State{btree: btree} = :sys.get_state(db)
    send(db, {:_test_check_in_reader, btree})

    :ok = CubDB.compact(db)

    assert_received :compaction_started
    refute_receive :clean_up_started

    assert %CubDB.State{clean_up_pending: true} = :sys.get_state(db)

    send(db, {:check_out_reader, btree})
    assert_receive :clean_up_started
  end

  test "set_auto_file_sync/1 configures auto file sync behavior", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir, auto_file_sync: true)

    assert %CubDB.State{auto_file_sync: true} = :sys.get_state(db)

    CubDB.set_auto_file_sync(db, false)

    assert %CubDB.State{auto_file_sync: false} = :sys.get_state(db)

    CubDB.set_auto_file_sync(db, true)

    assert %CubDB.State{auto_file_sync: true} = :sys.get_state(db)
  end

  test "file_sync/1 returns :ok", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    assert :ok = CubDB.file_sync(db)
  end

  test "data_dir/1 returns the path to the data directory", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    assert ^tmp_dir = CubDB.data_dir(db)
  end

  test "current_db_file/1 returns the path to the current database file", %{tmp_dir: tmp_dir} do
    {:ok, db} = CubDB.start_link(tmp_dir)
    expected_file_path = Path.join(tmp_dir, "0.cub")
    assert ^expected_file_path = CubDB.current_db_file(db)

    CubDB.subscribe(db)
    CubDB.compact(db)

    assert_receive :catch_up_completed
    assert_receive :clean_up_started

    expected_file_path = Path.join(tmp_dir, "1.cub")
    assert ^expected_file_path = CubDB.current_db_file(db)
  end
end
