defmodule CubDB.Store.CompactorTest do
  use ExUnit.Case

  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Compactor

  setup do
    tmp_dir = :os.cmd('mktemp -d') |> List.to_string |> String.trim |> String.to_charlist

    on_exit(fn ->
      with {:ok, files} <- File.ls(tmp_dir) do
        for file <- files, do: File.rm(Path.join(tmp_dir, file))
      end
      :ok = File.rmdir(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "start_link/3 runs compaction on a Btree and sends back the result", %{tmp_dir: tmp_dir} do
    entries = [foo: 1, bar: 2, baz: 3]
    btree = Enum.reduce(entries, Btree.new(Store.TestStore.new), fn {key, value}, btree ->
      Btree.insert(btree, key, value)
    end)
    store = Store.File.new(Path.join(tmp_dir, "1.compact"))

    {:ok, _pid} = Compactor.start_link(self(), btree, store)

    assert_receive {:compaction_completed, ^btree, compacted_btree}
    assert compacted_btree.size == btree.size
    assert Enum.to_list(compacted_btree) == Enum.to_list(btree)
  end
end
