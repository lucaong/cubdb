defmodule CubDB.Store.CompactorTest do
  use ExUnit.Case

  alias CubDB.Btree
  alias CubDB.Compactor
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

  test "run/3 runs compaction on a Btree and sends back the result", %{tmp_dir: tmp_dir} do
    entries = [foo: 1, bar: 2, baz: 3]
    {:ok, store} = Store.TestStore.create()

    btree =
      Enum.reduce(entries, Btree.new(store), fn {key, value}, btree ->
        Btree.insert(btree, key, value)
      end)

    {:ok, store} = Store.File.create(Path.join(tmp_dir, "1.compact"))

    Compactor.run(self(), btree, store)

    assert_receive {:compaction_completed, pid, ^btree, compacted_btree}, 1000
    assert pid == self()
    assert compacted_btree.size == btree.size
    assert Enum.to_list(compacted_btree) == Enum.to_list(btree)
  end
end
