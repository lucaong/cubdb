defmodule CubDB.Store.File do
  @moduledoc false

  alias CubDB.Store

  @type t :: %Store.File{pid: pid, file_path: binary}

  @enforce_keys [:pid, :file_path]
  defstruct [:pid, :file_path]

  def new(file_path) do
    with {:ok, pid} <- Agent.start_link(fn -> start(file_path) end) do
      %Store.File{pid: pid, file_path: file_path}
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
  alias CubDB.Store
  alias CubDB.Store.File.Blocks

  def put_node(%Store.File{pid: pid}, node) do
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

  def put_header(%Store.File{pid: pid}, header) do
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

  def sync(%Store.File{pid: pid}) do
    Agent.get(pid, fn {file, _} ->
      :file.sync(file)
    end)
  end

  def get_node(%Store.File{pid: pid}, location) do
    case Agent.get(pid, fn {file, _} ->
      read_term(file, location)
    end) do
      {:ok, term} -> term
      {:error, error} -> raise(error)
    end
  end

  def get_latest_header(%Store.File{pid: pid}) do
    Agent.get(pid, fn {file, pos} ->
      get_latest_good_header(file, pos)
    end)
  end

  def close(%Store.File{pid: pid}) do
    with :ok <-
           Agent.update(pid, fn {file, pos} ->
             :file.sync(file)
             {file, pos}
           end) do
      Agent.stop(pid)
    end
  end

  def blank?(%Store.File{file_path: path}) do
    case File.stat!(path) do
      %{size: 0} -> true
      _ -> false
    end
  end

  defp read_term(file, location) do
    with {:ok, <<length::32>>, len} <- read_blocks(file, location, 4),
         {:ok, bytes, _} <- read_blocks(file, location + len, length) do
      {:ok, deserialize(bytes)}
    else
      :eof -> {:error, %ArgumentError{message: "End of file"}}
    end
  rescue
    error -> {:error, error}
  end

  defp read_blocks(file, location, length) do
    length_with_markers = Blocks.length_with_markers(location, length)

    with {:ok, bin} <- :file.pread(file, location, length_with_markers) do
      bytes = Blocks.strip_markers(bin, location) |> Enum.join
      {:ok, bytes, length_with_markers}
    end
  end

  defp append_blocks(file, bytes, pos) do
    iolist = Blocks.add_markers(bytes, pos)

    with :ok <- :file.write(file, iolist) do
      {:ok, iolist_byte_size(iolist)}
    end
  end

  defp append_header(file, bytes, pos) do
    {loc, iolist} = Blocks.add_header_marker(bytes, pos)

    with :ok <- :file.write(file, iolist) do
      {:ok, loc, iolist_byte_size(iolist)}
    end
  end

  defp locate_latest_header(_, location) when location <= 0, do: nil

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

  defp get_latest_good_header(file, pos) do
    case locate_latest_header(file, pos) do
      nil -> nil
      location -> read_header(file, location)
    end
  end

  defp read_header(file, location) do
    case read_term(file, location) do
      {:ok, term} -> {location, term}
      {:error, _} -> get_latest_good_header(file, location - 1)
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

  defp iolist_byte_size(iolist) do
    iolist
    |> Enum.reduce(0, fn bytes, size -> size + byte_size(bytes) end)
  end
end
