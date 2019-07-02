defmodule CubDB.Store.FileTest do
  use CubDB.StoreExamples, async: true

  setup do
    tmp_path = :os.cmd('mktemp') |> List.to_string |> String.trim |> String.to_charlist
    store = CubDB.Store.File.new(tmp_path)

    on_exit(fn ->
      :file.delete(tmp_path)
    end)

    {:ok, store: store}
  end

  test "skips corrupted header and locates latest good header", %{store: store} do
    good_header = {1, 2, 3}
    CubDB.Store.put_header(store, good_header)

    CubDB.Store.put_header(store, {0, 0, 0})

    with {:ok, file} <- :file.open(store.file_path, [:read, :write, :raw, :binary]),
         {:ok, pos} <- :file.position(file, :eof),
         :ok <- :file.pwrite(file, pos - 7, "garbage") do
      assert {_, ^good_header} = CubDB.Store.get_latest_header(store)
    end
  end
end
