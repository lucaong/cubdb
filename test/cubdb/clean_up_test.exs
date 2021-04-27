defmodule CubDB.Store.CleanUpTest do
  use ExUnit.Case, async: true

  alias CubDB.Btree
  alias CubDB.CleanUp
  alias CubDB.Store

  setup do
    {tmp_dir, 0} = System.cmd("mktemp", ["-d"])
    tmp_dir = tmp_dir |> String.trim() |> String.to_charlist()

    on_exit(fn ->
      with {:ok, files} <- File.ls(tmp_dir) do
        for file <- files, do: File.rm(Path.join(tmp_dir, file))
      end

      :ok = File.rmdir(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "clean_up/2 removes all cubdb files older than the one used by the btree", %{
    tmp_dir: tmp_dir
  } do
    files = ["E.cub", "E.txt", "1.cub", "2.compact", "F.cub", "10.compact", "11.cub"]
    for file <- files, do: File.touch(Path.join(tmp_dir, file))

    {:ok, store} = Store.File.create(Path.join(tmp_dir, "F.cub"))
    btree = Btree.new(store)

    {:ok, pid} = CleanUp.start_link(tmp_dir)
    :ok = CleanUp.clean_up(pid, btree)

    # this is to wait for clean up to complete
    :sys.get_state(pid)

    {:ok, remaining_files} = File.ls(tmp_dir)
    assert Enum.sort(remaining_files) == ["10.compact", "11.cub", "E.txt", "F.cub"]
  end

  test "clean_up_old_compaction_files/2 removes all compaction files not used by the store", %{
    tmp_dir: tmp_dir
  } do
    files = ["0.cub", "0.txt", "1.compact", "2.compact", "4.compact"]
    for file <- files, do: File.touch(Path.join(tmp_dir, file))

    {:ok, store} = Store.File.create(Path.join(tmp_dir, "2.compact"))

    {:ok, pid} = CleanUp.start_link(tmp_dir)
    :ok = CleanUp.clean_up_old_compaction_files(pid, store)

    # this is to wait for clean up to complete
    :sys.get_state(pid)

    {:ok, remaining_files} = File.ls(tmp_dir)
    assert Enum.sort(remaining_files) == ["0.cub", "0.txt", "2.compact"]
  end
end
