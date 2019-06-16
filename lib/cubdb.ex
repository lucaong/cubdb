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
    @type t :: %CubDB.State{btree: Btree.t, data_dir: binary, compactor: pid | nil,
      clean_up: pid, clean_up_pending: boolean, busy_files: %{required(binary) => pos_integer}}

    @enforce_keys [:btree, :data_dir, :clean_up]
    defstruct btree: nil, data_dir: nil, compactor: nil, clean_up: nil,
      clean_up_pending: false, busy_files: %{}
  end

  def start_link(data_dir, options \\ []) do
    GenServer.start_link(__MODULE__, data_dir, options)
  end

  def inspect(pid) do
    GenServer.call(pid, :inspect)
  end

  @spec get(GenServer.server, any) :: any
  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  @spec has_key?(GenServer.server, any) :: {boolean, any}
  def has_key?(pid, key) do
    GenServer.call(pid, {:has_key?, key})
  end

  @spec select(GenServer.server, Keyword.t) :: {:ok, any} | {:error, Exception.t}
  def select(pid, options \\ []) when is_list(options) do
    GenServer.call(pid, {:select, options})
  end

  @spec size(GenServer.server) :: pos_integer
  def size(pid) do
    GenServer.call(pid, :size)
  end

  @spec put(GenServer.server, any, any) :: :ok
  def put(pid, key, value) do
    GenServer.call(pid, {:put, key, value})
  end

  @spec delete(GenServer.server, any) :: :ok
  def delete(pid, key) do
    GenServer.call(pid, {:delete, key})
  end

  @spec compact(GenServer.server) :: :ok | {:error, binary}
  def compact(pid) do
    GenServer.call(pid, :compact)
  end

  @spec cubdb_file?(binary) :: boolean

  def cubdb_file?(file_name) do
    file_extensions = [@db_file_extension, @compaction_file_extension]
    Enum.member?(file_extensions, Path.extname(file_name))
  end

  @spec db_file?(binary) :: boolean

  def db_file?(file_name) do
    Path.extname(file_name) == @db_file_extension
  end

  @spec compaction_file?(binary) :: boolean

  def compaction_file?(file_name) do
    Path.extname(file_name) == @compaction_file_extension
  end

  # OTP callbacks

  def init(data_dir) do
    case find_db_file(data_dir) do
      file_name when is_binary(file_name) or is_nil(file_name) ->
        store = Store.File.new(Path.join(data_dir, file_name || "0.#{@db_file_extension}"))
        {:ok, clean_up} = CleanUp.start_link(data_dir)
        {:ok, %State{btree: Btree.new(store), data_dir: data_dir, clean_up: clean_up}}

      {:error, reason} -> {:stop, reason}
    end
  end

  def handle_call(:inspect, _, state) do
    {:reply, state, state}
  end

  def handle_call(operation = {:get, _}, from, state = %State{btree: btree}) do
    state = read(from, btree, operation, state)
    {:noreply, state}
  end

  def handle_call(operation = {:has_key?, _}, from, state = %State{btree: btree}) do
    state = read(from, btree, operation, state)
    {:noreply, state}
  end

  def handle_call(operation = {:select, _}, from, state = %State{btree: btree}) do
    state = read(from, btree, operation, state)
    {:noreply, state}
  end

  def handle_call(operation = :size, from, state = %State{btree: btree}) do
    state = read(from, btree, operation, state)
    {:noreply, state}
  end

  def handle_call({:put, key, value}, _, state = %State{btree: btree}) do
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

  def handle_call(:compact, _, state = %State{btree: btree, data_dir: data_dir, clean_up: clean_up}) do
    reply = case can_compact?(state) do
      true ->
        {:ok, store} = new_compaction_store(data_dir)
        CleanUp.clean_up_old_compaction_files(clean_up, store)
        Compactor.start_link(self(), btree, store)

      {false, reason} ->
        {:error, reason}
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

  def handle_info({:catch_up, compacted_btree, original_btree}, state = %State{btree: latest_btree}) do
    if latest_btree == original_btree do
      compacted_btree = finalize_compaction(compacted_btree)
      state = %State{state | btree: compacted_btree, compactor: nil}
      {:noreply, trigger_clean_up(state)}
    else
      CatchUp.start_link(self(), compacted_btree, original_btree, latest_btree)
      {:noreply, state}
    end
  end

  def handle_info(:clean_up_completed, state) do
    {:noreply, %State{state | clean_up: nil}}
  end

  def handle_info({:check_out_reader, btree}, state = %State{clean_up_pending: clean_up_pending}) do
    state = check_out_reader(btree, state)

    state = if clean_up_pending == true,
      do: trigger_clean_up(state),
      else: state

    {:noreply, state}
  end

  defp read(from, btree, operation, state) do
    Reader.start_link(from, self(), btree, operation)
    check_in_reader(btree, state)
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

  defp new_compaction_store(data_dir) do
    with {:ok, file_names} <- File.ls(data_dir) do
      new_filename = file_names
      |> Enum.filter(&cubdb_file?/1)
      |> Enum.map(fn file_name -> Path.basename(file_name, Path.extname(file_name)) end)
      |> Enum.sort
      |> List.last
      |> String.to_integer(16)
      |> (&(&1 + 1)).()
      |> Integer.to_string(16)
      |> (&(&1 <> @compaction_file_extension)).()

      store = Store.File.new(Path.join(data_dir, new_filename))
      {:ok, store}
    end
  end

  defp can_compact?(%State{compactor: compactor}) do
    case compactor do
      nil -> true
      _ -> {false, :pending_compaction}
    end
  end

  defp check_in_reader(%Btree{store: store}, state = %State{busy_files: busy_files}) do
    %Store.File{file_path: file_path} = store
    busy_files = Map.update(busy_files, file_path, 1, &(&1 + 1))
    %State{state | busy_files: busy_files}
  end

  defp check_out_reader(%Btree{store: store}, state = %State{busy_files: busy_files}) do
    %Store.File{file_path: file_path} = store

    busy_files = case Map.get(busy_files, file_path) do
      n when n > 1 -> Map.update!(busy_files, file_path, &(&1 - 1))
      _ -> Map.delete(busy_files, file_path)
    end

    %State{state | busy_files: busy_files}
  end

  def trigger_clean_up(state) do
    if can_clean_up?(state),
      do: clean_up_now(state),
      else: clean_up_when_possible(state)
  end

  defp can_clean_up?(%State{btree: %Btree{store: store}, busy_files: busy_files}) do
    %Store.File{file_path: file_path} = store
    Enum.any?(busy_files, fn {file, _} -> file != file_path end) == false
  end

  defp clean_up_now(state = %State{btree: btree, clean_up: clean_up}) do
    :ok = CleanUp.clean_up(clean_up, btree)
    %State{state | clean_up_pending: false}
  end

  defp clean_up_when_possible(state) do
    %State{state | clean_up_pending: true}
  end
end
