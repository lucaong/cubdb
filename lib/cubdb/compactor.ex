defmodule CubDB.Compactor do
  use Task

  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.CleanUp

  @spec start_link(pid, %Btree{}, %Store.File{}, binary) :: {:ok, pid}

  def start_link(caller, btree, store, data_dir) do
    Task.start_link(__MODULE__, :run, [caller, btree, store, data_dir])
  end

  @spec run(pid, %Btree{}, %Store.File{}, binary) :: :ok

  def run(caller, btree, store, data_dir) do
    :ok = clean_up(data_dir, btree, store)
    compacted_btree = Btree.load(btree, store)
    send(caller, {:compaction_completed, btree, compacted_btree})
  end

  defp clean_up(data_dir, btree, %Store.File{file_path: file_path}) do
    exclude = [Path.basename(file_path)]
    clean_up = Task.async(CleanUp, :run, [data_dir, btree, exclude])
    Task.await(clean_up)
  end
end
