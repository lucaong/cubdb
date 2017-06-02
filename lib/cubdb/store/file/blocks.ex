defmodule CubDB.Store.File.Blocks do
  @block_size 4096

  def make(bin, loc, block_size \\ @block_size) do
  end

  def strip(bin, loc, block_size \\ @block_size) do
  end

  def read_length(loc, length, block_size \\ @block_size) do
    case rem(loc, block_size) do
      0 -> headers_length(length, block_size) + length
      r ->
        prefix = block_size - r
        rest = length - prefix
        prefix + headers_length(rest, block_size) + rest
    end
  end

  defp headers_length(length, block_size) do
    Float.ceil(length / (block_size - 1))
  end
end
