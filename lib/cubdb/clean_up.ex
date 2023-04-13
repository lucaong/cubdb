defmodule CubDB.CleanUp do
  @moduledoc false

  # The `CubDB.CleanUp` module takes care of cleaning up obsolete files, like
  # old database files after a compaction (and catch-up) completes, or old
  # compaction files left over by compactions that did not complete.
  #
  # It performs each clean-up operation in sequence.

  use GenServer

  alias CubDB.Store

  @type server :: GenServer.server()

  @spec start_link(String.t(), GenServer.options()) :: GenServer.on_start()

  def start_link(data_dir, options \\ []) do
    GenServer.start_link(__MODULE__, data_dir, options)
  end

  @spec clean_up(server, Store.File.t()) :: :ok

  def clean_up(pid, store) do
    GenServer.cast(pid, {:clean_up, store})
  end

  @spec clean_up_old_compaction_files(server, Store.File.t()) :: :ok

  def clean_up_old_compaction_files(pid, store) do
    GenServer.cast(pid, {:clean_up_old_compaction_files, store})
  end

  # OTP callbacks

  @impl true
  def init(data_dir) do
    {:ok, data_dir}
  end

  @impl true
  def handle_cast({:clean_up, %Store.File{file_path: latest_file_path}}, data_dir) do
    latest_file_name = Path.basename(latest_file_path)
    remove_older_files!(data_dir, latest_file_name)
    {:noreply, data_dir}
  end

  def handle_cast({:clean_up_old_compaction_files, %Store.File{file_path: file_path}}, data_dir) do
    current_compaction_file_name = Path.basename(file_path)
    remove_other_compaction_files!(data_dir, current_compaction_file_name)
    {:noreply, data_dir}
  end

  @spec remove_older_files!(String.t(), String.t()) :: :ok

  defp remove_older_files!(data_dir, latest_file_name) do
    latest_file_n = CubDB.file_name_to_n(latest_file_name)

    data_dir
    |> File.ls!()
    |> Enum.filter(&(CubDB.cubdb_file?(&1) && CubDB.file_name_to_n(&1) < latest_file_n))
    |> Enum.each(&File.rm!(Path.join(data_dir, &1)))
  end

  @spec remove_other_compaction_files!(String.t(), String.t()) :: :ok

  defp remove_other_compaction_files!(data_dir, file_name) do
    data_dir
    |> File.ls!()
    |> Enum.filter(&CubDB.compaction_file?/1)
    |> Enum.reject(&(&1 == file_name))
    |> Enum.each(&File.rm!(Path.join(data_dir, &1)))
  end
end
