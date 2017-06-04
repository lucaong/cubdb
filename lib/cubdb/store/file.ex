defmodule CubDB.Store.File do
  @moduledoc """
  Append-only file-based store implementation
  """

  defstruct [:pid, :file, :file_path]
  alias CubDB.Store.File

  def new(file_path) do
    with {:ok, pid} <- Agent.start_link(fn -> nil end) do
      open(pid, file_path)
    end
  end

  defp open(pid, file_path) do
    Agent.get_and_update(pid, fn _ ->
      with {:ok, file} <- :file.open(file_path, [:read, :append, :raw, :binary]),
           {:ok, pos} <- :file.position(file, :eof) do
        {%File{pid: pid, file: file, file_path: file_path}, {file, pos}}
      end
    end)
  end
end

defimpl CubDB.Store, for: CubDB.Store.File do
  alias CubDB.Store.File
  alias CubDB.Store.File.Blocks

  def put_node(%File{pid: pid}, node) do
    Agent.get_and_update(pid, fn {file, pos} ->
      bytes = serialize(node)
      case append_blocks(file, bytes, pos) do
        {:ok, written_size} -> {pos, {file, pos + written_size}}
        _ ->
          {:ok, pos} = :file.position(file, :eof)
          {{:error, "Write error"}, {file, pos}}
      end
    end)
  end

  def get_node(%File{pid: pid}, location) do
    Agent.get(pid, fn {file, _} ->
      with {:ok, <<length::32>>, len} <- read_blocks(file, location, 4),
           {:ok, bytes, _} <- read_blocks(file, location + len, length) do
             deserialize(bytes)
      else
        :eof -> {:error, "End of file"}
      end
    end)
  end

  def get_latest_header(%File{}) do
  end

  defp read_blocks(file, location, length) do
    length_with_headers = Blocks.length_with_headers(location, length)
    with {:ok, bin} <- :file.pread(file, location, length_with_headers) do
      {:ok, Blocks.strip_headers(bin, location), length_with_headers}
    end
  end

  defp append_blocks(file, bytes, pos) do
    bytes_with_headers = Blocks.add_headers(bytes, pos)
    with :ok <- :file.write(file, bytes_with_headers) do
      {:ok, byte_size(bytes_with_headers)}
    end
  end

  defp serialize(node) do
    node_bytes = :erlang.term_to_binary(node)
    size = byte_size(node_bytes)
    <<size::32>> <> node_bytes
  end

  defp deserialize(bytes) do
    :erlang.binary_to_term(bytes)
  end
end
