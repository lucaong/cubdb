defmodule CubDB.CleanUp do
  use Task

  alias CubDB.Btree
  alias CubDB.Store

  @spec start_link(binary, %Btree{}, [binary]) :: {:ok, pid}

  def start_link(data_dir, btree, exclude \\ []) do
    Task.start_link(__MODULE__, :run, [data_dir, btree, exclude])
  end

  @spec run(binary, %Btree{}, [binary]) :: :ok | {:error, any}

  def run(data_dir, btree, exclude \\ []) do
    clean_up(data_dir, btree, exclude)
  end

  defp clean_up(data_dir, btree, exclude) do
    %Btree{store: %Store.File{file_path: latest_file_path}} = btree
    latest_file_name = Path.basename(latest_file_path)
    exclude = [latest_file_name | exclude]

    with {:ok, file_names} <- File.ls(data_dir) do
      file_names
      |> Enum.filter(&cubdb_file?/1)
      |> Enum.reject(&(Enum.member?(exclude, &1)))
      |> Enum.reduce(:ok, fn file, _ ->
        :ok = File.rm(Path.join(data_dir, file))
      end)
    end
  end

  defp cubdb_file?(file_name) do
    file_extensions = [CubDB.db_file_extension, CubDB.compaction_file_extension]
    Enum.member?(file_extensions, Path.extname(file_name))
  end
end
