defmodule CubDB.Store.FileTest do
  use CubDB.StoreExamples, async: true

  setup do
    tmp_path = :os.cmd('mktemp') |> List.to_string |> String.trim |> String.to_charlist
    store = CubDB.Store.File.new(tmp_path)

    on_exit(fn ->
      :file.delete(tmp_path)
    end)

    {:ok, store: store, file_path: tmp_path}
  end

  test "new/1 returns a Store.File on the given file path", %{file_path: file_path} do
    store = CubDB.Store.File.new(file_path)
    assert %CubDB.Store.File{pid: pid, file_path: ^file_path} = store
    assert Process.alive?(pid)
  end

  test "skips corrupted header and locates latest good header", %{store: store} do
    good_header = {1, 2, 3}
    CubDB.Store.put_header(store, good_header)

    CubDB.Store.put_header(store, {0, 0, 0})

    # corrupt the last header
    with {:ok, file} <- :file.open(store.file_path, [:read, :write, :raw, :binary]),
         {:ok, pos} <- :file.position(file, :eof),
         :ok <- :file.pwrite(file, pos - 7, "garbage") do
      assert {_, ^good_header} = CubDB.Store.get_latest_header(store)
    end
  end

  test "skips truncated header and locates latest good header", %{store: store} do
    good_header = {1, 2, 3}
    CubDB.Store.put_header(store, good_header)

    CubDB.Store.put_header(store, {0, 0, 0})

    # truncate the last header
    with {:ok, file} <- :file.open(store.file_path, [:read, :write, :raw, :binary]),
         {:ok, pos} <- :file.position(file, :eof),
         :ok <- :file.position(file, pos - 1),
         :ok <- :file.truncate(file) do
      assert {_, ^good_header} = CubDB.Store.get_latest_header(store)
    end
  end

  test "close/1 stops the agent", %{store: store} do
    %CubDB.Store.File{pid: pid} = store

    assert Process.alive?(pid) == true

    CubDB.Store.close(store)

    assert Process.alive?(pid) == false
  end
end
