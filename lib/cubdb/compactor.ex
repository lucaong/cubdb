defmodule CubDB.Compactor do
  use Task

  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.CleanUp

  def start_link(caller, btree, data_dir) do
    Task.start_link(__MODULE__, :run, [caller, btree, data_dir])
  end

  def run(caller, btree, data_dir) do
    clean_up = Task.async(CleanUp, :run, [data_dir, btree])
    :ok = Task.await(clean_up)
    store = new_compaction_store(data_dir, btree)
    compacted_btree = Btree.load(btree, store)
    send(caller, {:compaction_completed, btree, compacted_btree})
  end

  defp new_compaction_store(data_dir, %Btree{store: %Store.File{file_path: file_path}}) do
    new_filename =
      file_path
      |> Path.basename(CubDB.db_file_extension)
      |> String.to_integer(16)
      |> (&(&1 + 1)).()
      |> Integer.to_string(16)
      |> (&(&1 <> CubDB.compaction_file_extension)).()
    Store.File.new(Path.join(data_dir, new_filename))
  end
end
