defmodule CubDB.CleanUp do
  use Task

  alias CubDB.Btree
  alias CubDB.Store

  def start_link(data_dir, btree) do
    Task.start_link(__MODULE__, :run, [data_dir, btree])
  end

  def run(data_dir, btree) do
    clean_up(data_dir, btree)
  end

  def clean_up(data_dir, btree) do
    %Btree{store: %Store.File{file_path: latest_file_path}} = btree
    latest_file_name = Path.basename(latest_file_path)

    with {:ok, files} <- File.ls(data_dir),
         files_to_delete <- Enum.filter(files, &(&1 != latest_file_name)) do
      Enum.reduce(files_to_delete, :ok, fn file, _ ->
        :ok = File.rm(Path.join(data_dir, file))
      end)
    end
  end
end
