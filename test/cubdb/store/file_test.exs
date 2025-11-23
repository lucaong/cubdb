defmodule CubDB.Store.FileTest do
  use ExUnit.Case, async: true
  use CubDB.StoreExamples
  require TestHelper

  import CubDB.Btree, only: [header: 1]

  setup do
    {tmp_path, 0} = System.cmd("mktemp", [])
    tmp_path = tmp_path |> String.trim()

    {:ok, store} = CubDB.Store.File.create(tmp_path)

    on_exit(fn ->
      :file.delete(tmp_path |> String.to_charlist())
    end)

    {:ok, store: store, file_path: tmp_path}
  end

  test "start_link/1 starts a Store.File on the given file path", %{
    file_path: file_path,
    store: store
  } do
    assert %CubDB.Store.File{pid: pid, file_path: ^file_path} = store
    assert Process.alive?(pid)
  end

  test "get_latest_header/1 skips corrupted header and locates latest good header", %{
    store: store
  } do
    good_header = header(size: 1, location: 2, dirt: 3, metadata: [])
    CubDB.Store.put_header(store, good_header)

    CubDB.Store.put_header(store, header(size: 0, location: 0, dirt: 0, metadata: []))

    # corrupt the last header
    {:ok, file} = :file.open(store.file_path, [:read, :write, :raw, :binary])
    {:ok, pos} = :file.position(file, :eof)
    :ok = :file.pwrite(file, pos - 7, "garbage")

    assert {_, ^good_header} = CubDB.Store.get_latest_header(store)
  end

  test "get_latest_header/1 skips truncated header and locates latest good header", %{
    store: store
  } do
    good_header = header(size: 1, location: 2, dirt: 3, metadata: [])
    CubDB.Store.put_header(store, good_header)

    CubDB.Store.put_header(store, header(size: 0, location: 0, dirt: 0, metadata: []))

    # truncate the last header
    {:ok, file} = :file.open(store.file_path, [:read, :write, :raw, :binary])
    {:ok, pos} = :file.position(file, :eof)
    {:ok, _} = :file.position(file, pos - 1)
    :ok = :file.truncate(file)

    assert {_, ^good_header} = CubDB.Store.get_latest_header(store)
  end

  test "get_latest_header/1 skips data and locates latest good header", %{store: store} do
    header = header(size: 1, location: 2, dirt: 3, metadata: [])
    CubDB.Store.put_header(store, header)

    data_longer_than_one_block = String.duplicate("x", 1030)

    CubDB.Store.put_node(store, data_longer_than_one_block)

    assert {_, ^header} = CubDB.Store.get_latest_header(store)
  end

  test "close/1 stops the agent", %{store: store} do
    %CubDB.Store.File{pid: pid} = store

    assert Process.alive?(pid) == true

    CubDB.Store.close(store)

    assert Process.alive?(pid) == false
  end

  test "open?/1 returns true if the agent is alive, false otherwise", %{store: store} do
    assert CubDB.Store.open?(store) == true

    CubDB.Store.close(store)

    assert CubDB.Store.open?(store) == false
  end

  test "returns error if the same file is already in use by another store", %{
    file_path: file_path
  } do
    TestHelper.trapping_exit do
      assert {:error, {%ArgumentError{message: message}, _}} = CubDB.Store.File.create(file_path)
      assert message == "file \"#{file_path}\" is already in use by another CubDB.Store.File"
    end
  end
end
