defmodule CubDB.Store.File do
  @moduledoc false

  # `CubDB.Store.File` is the main implementation of the `CubDB.Store` protocol,
  # based on an append-only file. In order to be able to locate the latest
  # header, the file is divided in blocks of the same byte size, each beginning
  # with a one-byte marker at the beginning (utilities to deal with blocks are
  # in the `CubDB.Store.File.Blocks` module). The block marker indicates whether
  # a block is a data block or a header block. Headers are only written at the
  # beginning of a header block.
  #
  # The file is never updated in-place, and all updates are appended at the end
  # of the file. When the database starts, the data file is traversed backwards
  # block by block until the latest readable header is located. This allows
  # operations to be atomic, and makes the database robust to corruption due to
  # sudden shutdowns.

  alias CubDB.Store

  @type state :: {:file.io_device(), integer}

  @type t :: %Store.File{pid: pid, file_path: String.t()}

  @enforce_keys [:pid, :file_path]
  defstruct [:pid, :file_path]

  @spec create(String.t(), keyword()) :: {:ok, t} | {:error, term}

  def create(file_path, options \\ []) do
    with {:ok, pid} <- Agent.start_link(fn -> init(file_path) end, options) do
      {:ok, %Store.File{pid: pid, file_path: file_path}}
    end
  end

  @spec init(String.t()) :: state

  defp init(file_path) do
    ensure_exclusive_access!(file_path)
    {:ok, file} = :file.open(file_path, [:read, :append, :raw, :binary])
    {:ok, pos} = :file.position(file, :eof)

    {file, pos}
  end

  @spec ensure_exclusive_access!(String.t()) :: nil

  defp ensure_exclusive_access!(file_path) do
    unless :global.set_lock({{__MODULE__, file_path}, self()}, [node()], 0) do
      raise ArgumentError,
        message: "file \"#{file_path}\" is already in use by another CubDB.Store.File"
    end
  end
end

defimpl CubDB.Store, for: CubDB.Store.File do
  alias CubDB.Store
  alias CubDB.Store.File.Blocks

  def put_node(%Store.File{pid: pid}, node) do
    Agent.get_and_update(
      pid,
      fn {file, pos} ->
        bytes = serialize(node)

        case append_blocks(file, bytes, pos) do
          {:ok, written_size} ->
            {{:ok, pos}, {file, pos + written_size}}

          error ->
            {:ok, pos} = :file.position(file, :eof)
            {error, {file, pos}}
        end
      end,
      :infinity
    )
    |> raise_if_error()
  end

  def put_header(%Store.File{pid: pid}, header) do
    Agent.get_and_update(
      pid,
      fn {file, pos} ->
        header_bytes = serialize(header)

        case append_header(file, header_bytes, pos) do
          {:ok, loc, written_size} ->
            {{:ok, loc}, {file, pos + written_size}}

          error ->
            {:ok, pos} = :file.position(file, :eof)
            {error, {file, pos}}
        end
      end,
      :infinity
    )
    |> raise_if_error()
  end

  def sync(%Store.File{pid: pid}) do
    Agent.get(
      pid,
      fn {file, _} ->
        :file.datasync(file)
      end,
      :infinity
    )
  end

  def get_node(%Store.File{pid: pid}, location) do
    Agent.get(
      pid,
      fn {file, _} ->
        read_term(file, location)
      end,
      :infinity
    )
    |> raise_if_error()
  end

  def get_latest_header(%Store.File{pid: pid}) do
    Agent.get(
      pid,
      fn {file, pos} ->
        get_latest_good_header(file, pos)
      end,
      :infinity
    )
  end

  def close(%Store.File{pid: pid}) do
    with :ok <-
           Agent.update(
             pid,
             fn {file, pos} ->
               :file.sync(file)
               {file, pos}
             end,
             :infinity
           ) do
      Agent.stop(pid, :normal, :infinity)
    end
  end

  def blank?(%Store.File{file_path: path}) do
    case File.stat!(path) do
      %{size: 0} -> true
      _ -> false
    end
  end

  def open?(%Store.File{pid: pid}) do
    Process.alive?(pid)
  end

  defp raise_if_error({:ok, value}), do: value

  defp raise_if_error({:error, :enospc}), do: raise("No space left on device")

  defp raise_if_error({:error, error})
       # is_exception is only available from Elixir 1.11
       when is_map(error) and :erlang.is_map_key(:__exception__, error),
       do: raise(error)

  defp raise_if_error({:error, error}), do: raise("File error: #{inspect(error)}")

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
      bytes = Blocks.strip_markers(bin, location) |> Enum.join()
      {:ok, bytes, length_with_markers}
    end
  end

  defp append_blocks(file, bytes, pos) do
    iolist = Blocks.add_markers(bytes, pos)

    with :ok <- :file.write(file, iolist) do
      {:ok, :erlang.iolist_size(iolist)}
    end
  end

  defp append_header(file, bytes, pos) do
    {loc, iolist} = Blocks.add_header_marker(bytes, pos)

    with :ok <- :file.write(file, iolist) do
      {:ok, loc, :erlang.iolist_size(iolist)}
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
end
