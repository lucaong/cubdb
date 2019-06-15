defmodule CubDB do
  @moduledoc """
  Documentation for CubDB.
  """

  use GenServer

  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Reader
  alias CubDB.Compactor
  alias CubDB.CatchUp
  alias CubDB.CleanUp

  @db_file_extension ".cub"
  @compaction_file_extension ".compact"

  defmodule State do
    @enforce_keys [:btree, :data_dir]
    defstruct btree: nil, data_dir: nil, compactor: nil
  end

  def start_link(data_dir, options \\ []) do
    GenServer.start_link(__MODULE__, data_dir, options)
  end

  @spec get(GenServer.server, any) :: any
  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  @spec has_key?(GenServer.server, any) :: {boolean, any}
  def has_key?(pid, key) do
    GenServer.call(pid, {:has_key?, key})
  end

  @spec size(GenServer.server) :: pos_integer
  def size(pid) do
    GenServer.call(pid, :size)
  end

  @spec set(GenServer.server, any, any) :: :ok
  def set(pid, key, value) do
    GenServer.call(pid, {:set, key, value})
  end

  @spec delete(GenServer.server, any) :: :ok
  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  @spec compact(GenServer.server) :: :ok | {:error, binary}
  def compact(pid) do
    GenServer.call(pid, :compact)
  end

  def db_file_extension, do: @db_file_extension

  def compaction_file_extension, do: @compaction_file_extension

  # OTP callbacks

  def init(data_dir) do
    case find_db_file(data_dir) do
      file_name when is_binary(file_name) ->
        store = Store.File.new(Path.join(data_dir, file_name))
        {:ok, %State{btree: Btree.new(store), data_dir: data_dir}}

      nil ->
        store = Store.File.new(Path.join(data_dir, "0.cub"))
        {:ok, %State{btree: Btree.new(store), data_dir: data_dir}}

      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_call(operation = {:get, _}, from, state = %State{btree: btree}) do
    Reader.start_link(from, btree, operation)
    {:noreply, state}
  end

  def handle_call(operation = {:has_key?, _}, from, state = %State{btree: btree}) do
    Reader.start_link(from, btree, operation)
    {:noreply, state}
  end

  def handle_call(operation = :size, from, state = %State{btree: btree}) do
    Reader.start_link(from, btree, operation)
    {:noreply, state}
  end

  def handle_call({:set, key, value}, _, state = %State{btree: btree}) do
    btree = Btree.insert(btree, key, value)
    {:reply, :ok, %State{state | btree: btree}}
  end

  def handle_call({:delete, key}, _, state = %State{btree: btree, compactor: compactor}) do
    btree = case compactor do
      nil -> Btree.delete(btree, key)
      _ -> Btree.mark_deleted(btree, key)
    end
    {:reply, :ok, %State{state | btree: btree}}
  end

  def handle_call(:latest_btree, _, state = %State{btree: btree}) do
    {:reply, btree, state}
  end

  def handle_call(:compact, _, state = %State{compactor: compactor, btree: btree, data_dir: data_dir}) do
    reply = case compactor do
      nil ->
        Compactor.start_link(self(), btree, data_dir)

      _ ->
        {:error, "compaction already in progress"}
    end

    case reply do
      {:ok, compactor} -> {:reply, :ok, %State{state | compactor: compactor}}
      error -> {:reply, error, state}
    end
  end

  def handle_info({:compaction_completed, original_btree, compacted_btree}, state) do
    send(self(), {:catch_up, compacted_btree, original_btree})
    {:noreply, state}
  end

  def handle_info({:catch_up, compacted_btree, original_btree}, state = %State{data_dir: data_dir, btree: latest_btree}) do
    if latest_btree == original_btree do
      compacted_btree = finalize_compaction(compacted_btree)
      # TODO delay actual deletion when no reader references old file
      CleanUp.start_link(data_dir, compacted_btree)
      {:noreply, %State{state | btree: compacted_btree, compactor: nil}}
    else
      CatchUp.start_link(self(), compacted_btree, original_btree, latest_btree)
      {:noreply, state}
    end
  end

  defp find_db_file(data_dir) do
    with :ok <- File.mkdir_p(data_dir),
         {:ok, files} <- File.ls(data_dir) do
      files
      |> Enum.filter(&String.ends_with?(&1, @db_file_extension))
      |> Enum.sort
      |> List.last
    end
  end

  defp finalize_compaction(%Btree{store: %Store.File{file_path: file_path}}) do
    new_path = String.replace_suffix(file_path, @compaction_file_extension, @db_file_extension)
    :ok = File.rename(file_path, new_path)

    store = Store.File.new(new_path)
    Btree.new(store)
  end
end
