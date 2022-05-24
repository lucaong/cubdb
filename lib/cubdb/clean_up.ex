defmodule CubDB.CleanUp do
  @moduledoc false

  # The `CubDB.CleanUp` module takes care of cleaning up obsolete files, like
  # old database files after a compaction (and catch-up) completes, or old
  # compaction files left over by compactions that did not complete.
  #
  # It performs each clean-up operation in sequence.

  use GenServer

  alias CubDB.Btree
  alias CubDB.Store

  @spec start_link(binary, Keyword.t()) :: GenServer.on_start()

  def start_link(data_dir, options \\ []) do
    GenServer.start_link(__MODULE__, data_dir, options)
  end

  @spec clean_up(GenServer.server(), Btree.t()) :: :ok

  def clean_up(pid, btree) do
    GenServer.cast(pid, {:clean_up, btree})
  end

  @spec clean_up_old_compaction_files(GenServer.server(), Store.File.t()) :: :ok

  def clean_up_old_compaction_files(pid, store) do
    GenServer.cast(pid, {:clean_up_old_compaction_files, store})
  end

  # OTP callbacks

  def init(data_dir) do
    {:ok, data_dir}
  end

  def handle_cast({:clean_up, %Btree{store: store}}, data_dir) do
    %Store.File{file_path: latest_file_path} = store
    latest_file_name = Path.basename(latest_file_path)
    :ok = remove_older_files(data_dir, latest_file_name)
    {:noreply, data_dir}
  end

  def handle_cast({:clean_up_old_compaction_files, %Store.File{file_path: file_path}}, data_dir) do
    current_compaction_file_name = Path.basename(file_path)
    :ok = remove_other_compaction_files(data_dir, current_compaction_file_name)
    {:noreply, data_dir}
  end

  defp remove_older_files(data_dir, latest_file_name) do
    latest_file_n = CubDB.file_name_to_n(latest_file_name)

    with {:ok, file_names} <- File.ls(data_dir) do
      file_names
      |> Enum.filter(fn file_name ->
        CubDB.cubdb_file?(file_name) && CubDB.file_name_to_n(file_name) < latest_file_n
      end)
      |> Enum.reduce(:ok, fn file_name, _ ->
        :ok = File.rm(Path.join(data_dir, file_name))
      end)
    end
  end

  defp remove_other_compaction_files(data_dir, file_name) do
    with {:ok, files} <- File.ls(data_dir) do
      files
      |> Enum.filter(&CubDB.compaction_file?/1)
      |> Enum.reject(&(&1 == file_name))
      |> Enum.reduce(:ok, fn file, _ ->
        :ok = File.rm(Path.join(data_dir, file))
      end)
    end
  end
end
