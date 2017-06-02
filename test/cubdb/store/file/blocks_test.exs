defmodule CubDB.Store.File.BlocksTest do
  use ExUnit.Case

  alias CubDB.Store.File.Blocks

  test "read_length/3 computes the actual length including block headers" do
    #  *
    # |x|.| | |
    assert Blocks.read_length(0, 1, 4) == 2
    #    *
    # |x|.|.|.|
    assert Blocks.read_length(1, 3, 4) == 3
    #  *
    # |x|.|.|.|x|.|.|.|x|.|.| |
    assert Blocks.read_length(0, 8, 4) == 11
    #    *
    # |x|.|.|.|x|.|.|.|x|.|.| |
    assert Blocks.read_length(1, 8, 4) == 10
    #      *
    # |x| |.|.|x|.|.|.|x|.|.|.|
    assert Blocks.read_length(2, 8, 4) == 10
    #        *
    # |x| | |.|x|.|.|.|x|.|.|.|x|.|
    assert Blocks.read_length(3, 8, 4) == 11
    #  *
    # |x|.|.|.|x|.|.|.|x|.|.|.|x|.|
    assert Blocks.read_length(0, 10, 4) == 14
  end
end
