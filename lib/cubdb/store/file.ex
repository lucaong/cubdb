defmodule CubDB.Store.File do
  @moduledoc """
  Append-only file-based store implementation
  """

  defstruct [:pid, :file_path]
  alias CubDB.Store.File

  def new(file_path) do
    with {:ok, pid} <- Agent.start_link(fn -> start(file_path) end) do
      %File{pid: pid, file_path: file_path}
    end
  end

  defp start(file_path) do
    with {:ok, file} <- :file.open(file_path, [:read, :append, :raw, :binary]),
         {:ok, pos} <- :file.position(file, :eof) do
      {file, pos}
    end
  end
end

defimpl CubDB.Store, for: CubDB.Store.File do
  alias CubDB.Store.File
  alias CubDB.Store.File.Blocks

  def put_node(%File{pid: pid}, node) do
    Agent.get_and_update(pid, fn {file, pos} ->
      bytes = serialize(node)

      case append_blocks(file, bytes, pos) do
        {:ok, written_size} ->
          {pos, {file, pos + written_size}}

        _ ->
          {:ok, pos} = :file.position(file, :eof)
          {{:error, "Write error"}, {file, pos}}
      end
    end)
  end

  def put_header(%File{pid: pid}, header) do
    Agent.get_and_update(pid, fn {file, pos} ->
      header_bytes = serialize(header)

      case append_header(file, header_bytes, pos) do
        {:ok, loc, written_size} ->
          {loc, {file, pos + written_size}}

        _ ->
          {:ok, pos} = :file.position(file, :eof)
          {{:error, "Write error"}, {file, pos}}
      end
    end)
  end

  def commit(%File{pid: pid}) do
    Agent.get(pid, fn {file, _} ->
      :file.sync(file)
    end)
  end

  def get_node(%File{pid: pid}, location) do
    Agent.get(pid, fn {file, _} ->
      read_term(file, location)
    end)
  end

  def get_latest_header(%File{pid: pid}) do
    Agent.get(pid, fn {file, pos} ->
      case locate_latest_header(file, pos) do
        nil -> nil
        location -> {location, read_term(file, location)}
      end
    end)
  end

  def close(%File{pid: pid}) do
    with :ok <-
           Agent.update(pid, fn {file, pos} ->
             :file.sync(file)
             {file, pos}
           end) do
      Agent.stop(pid)
    end
  end

  defp read_term(file, location) do
    with {:ok, <<length::32>>, len} <- read_blocks(file, location, 4),
         {:ok, bytes, _} <- read_blocks(file, location + len, length) do
      deserialize(bytes)
    else
      :eof -> {:error, "End of file"}
    end
  end

  defp read_blocks(file, location, length) do
    length_with_headers = Blocks.length_with_headers(location, length)

    with {:ok, bin} <- :file.pread(file, location, length_with_headers) do
      {:ok, Blocks.strip_markers(bin, location), length_with_headers}
    end
  end

  defp append_blocks(file, bytes, pos) do
    bytes_with_headers = Blocks.add_markers(bytes, pos)

    with :ok <- :file.write(file, bytes_with_headers) do
      {:ok, byte_size(bytes_with_headers)}
    end
  end

  defp append_header(file, bytes, pos) do
    {loc, bytes_with_marker} = Blocks.add_header_marker(bytes, pos)

    with :ok <- :file.write(file, bytes_with_marker) do
      {:ok, loc, byte_size(bytes_with_marker)}
    end
  end

  defp locate_latest_header(_, location) when location == 0, do: nil

  defp locate_latest_header(file, location) do
    loc = Blocks.latest_possible_header(location)

    with {:ok, <<marker::8>>} <- :file.pread(file, loc, 1) do
      if Blocks.header_marker?(marker) do
        loc
      else
        locate_latest_header(file, loc)
      end
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
