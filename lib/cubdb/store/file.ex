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

  def put_node(%File{pid: pid}, node) do
    Agent.get_and_update(pid, fn {file, pos} ->
      {bytes, size} = serialize(node, pos)
      :ok = :file.pwrite(file, :eof, bytes)
      {pos, {file, pos + size}}
    end)
  end

  def get_node(%File{pid: pid}, location) do
    Agent.get(pid, fn {file, _} ->
      with {:ok, <<length::32>>} <- :file.pread(file, location, 4),
           {:ok, bytes} <- :file.pread(file, location + 4, length) do
             deserialize(bytes)
      else
        :eof -> {:error, "End of file"}
      end
    end)
  end

  def get_latest_header(%File{}) do
  end

  defp serialize(node, _) do
    node_bytes = :erlang.term_to_binary(node)
    size = byte_size(node_bytes)
    {<<size::32>> <> node_bytes, size + 4}
  end

  defp deserialize(bytes) do
    :erlang.binary_to_term(bytes)
  end
end
