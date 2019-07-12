defmodule PropertyBased.CubDb.Store.File.BlocksTest do
  use ExUnit.Case
  use Quixir

  alias CubDB.Store.File.Blocks

  @tag property_based: true
  test "adding and removing block headers" do
    ptest [
      block_size: int(min: 4, max: 32),
      bin: string(min: 1, max: 64, chars: :ascii),
      loc: int(min: 0, max: 256)
    ], repeat_for: 100 do
      bin_with_headers = Blocks.add_markers(bin, loc, block_size) |> Enum.join
      assert Blocks.length_with_headers(loc, byte_size(bin), block_size) ==
        byte_size(bin_with_headers)
      assert Blocks.strip_markers(bin_with_headers, loc, block_size) |> Enum.join ==
        bin
    end
  end
end
