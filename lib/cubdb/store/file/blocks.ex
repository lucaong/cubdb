defmodule CubDB.Store.File.Blocks do
  @moduledoc false

  # `CubDB.Store.File.Blocks` contains helper functions to deal with file
  # blocks. In order to efficiently locate the latest readable header, the file
  # is divided in blocks of 1024 bytes. Each block starts with a 1-byte marker,
  # that indicates whether the block is a data block or a header block. Headers
  # can only be written at the beginning of a header block, so when appending a
  # header, the remaining space of the previous block is padded with 0s.
  #
  # This module takes care of adding/stripping block markers to/from a binary,
  # computing the possible location of headers, and computing the length of a
  # binary after adding block markers.

  @block_size 1024
  @data_marker 0
  @header_marker 42

  @spec add_markers(binary, non_neg_integer, non_neg_integer) :: [binary]

  def add_markers(bin, loc, block_size \\ @block_size) do
    at_block_boundary(bin, loc, block_size, &add/3)
  end

  @spec strip_markers(binary, non_neg_integer, non_neg_integer) :: [binary]

  def strip_markers(bin, loc, block_size \\ @block_size) do
    at_block_boundary(bin, loc, block_size, &strip/3)
  end

  @spec length_with_markers(non_neg_integer, non_neg_integer, non_neg_integer) :: non_neg_integer

  def length_with_markers(loc, length, block_size \\ @block_size) do
    case rem(loc, block_size) do
      0 ->
        trunc(markers_length(length, block_size) + length)

      r ->
        prefix = block_size - r
        rest = length - prefix
        trunc(prefix + markers_length(rest, block_size) + rest)
    end
  end

  @spec add_header_marker(binary, non_neg_integer, non_neg_integer) :: {non_neg_integer, [binary]}

  def add_header_marker(bin, loc, block_size \\ @block_size) do
    case rem(loc, block_size) do
      0 ->
        {loc, [<<@header_marker>> | add_markers(bin, loc + 1, block_size)]}

      r ->
        block_rest = block_size - r
        padding = String.pad_leading(<<>>, block_rest, <<@data_marker>>)
        header_bytes = add_markers(bin, loc + block_rest + 1, block_size)
        {loc + block_rest, [padding | [<<@header_marker>> | header_bytes]]}
    end
  end

  @spec latest_possible_header(non_neg_integer, non_neg_integer) :: non_neg_integer

  def latest_possible_header(loc, block_size \\ @block_size) do
    div(loc - 1, block_size) * block_size
  end

  @spec header_marker?(byte) :: boolean

  def header_marker?(marker), do: @header_marker == marker

  defp at_block_boundary(bin, loc, block_size, function) do
    case rem(loc, block_size) do
      0 ->
        function.(bin, [], block_size)

      r ->
        block_rest = block_size - r

        if byte_size(bin) <= block_rest do
          [bin]
        else
          <<prefix::binary-size(block_rest), rest::binary>> = bin
          function.(rest, [prefix], block_size)
        end
    end
  end

  defp add(bin, acc, block_size) do
    data_size = block_size - 1

    if byte_size(bin) <= data_size do
      [bin | [<<@data_marker>> | acc]] |> Enum.reverse()
    else
      <<block::binary-size(data_size), rest::binary>> = bin
      add(rest, [block | [<<@data_marker>> | acc]], block_size)
    end
  end

  defp strip(bin, acc, block_size) do
    if byte_size(bin) <= block_size do
      <<_::binary-1, block::binary>> = bin
      [block | acc] |> Enum.reverse()
    else
      data_size = block_size - 1
      <<_::binary-1, block::binary-size(data_size), rest::binary>> = bin
      strip(rest, [block | acc], block_size)
    end
  end

  defp markers_length(length, block_size) do
    Float.ceil(length / (block_size - 1))
  end
end
