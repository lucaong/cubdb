defmodule CubDB.Store.File.BlocksTest do
  use ExUnit.Case

  import CubDB.Store.File.Blocks

  test "length_with_headers/3 returns an integer" do
    assert is_integer(length_with_headers(0, 1, 4))
  end

  test "length_with_headers/3 computes length including block headers" do
    #  - -
    # |x|.| | |
    assert length_with_headers(0, 1, 4) == 2
    #    - -
    # |x|.|.|.|
    assert length_with_headers(1, 3, 4) == 3
    #  - - - - - - - - - - -
    # |x|.|.|.|x|.|.|.|x|.|.| |
    assert length_with_headers(0, 8, 4) == 11
    #    - - - - - - - - - -
    # |x|.|.|.|x|.|.|.|x|.|.| |
    assert length_with_headers(1, 8, 4) == 10
    #      - - - - - - - - - -
    # |x| |.|.|x|.|.|.|x|.|.|.|
    assert length_with_headers(2, 8, 4) == 10
    #        - - - - - - - - - - -
    # |x| | |.|x|.|.|.|x|.|.|.|x|.|
    assert length_with_headers(3, 8, 4) == 11
    #  - - - - - - - - - - - - - -
    # |x|.|.|.|x|.|.|.|x|.|.|.|x|.|
    assert length_with_headers(0, 10, 4) == 14
  end

  test "strip_headers/3 removes the block headers" do
    assert <<1, 2, 3>> =
      strip_headers(<<0, 1, 2, 3>>, 0, 4)

    assert <<1, 2>> =
      strip_headers(<<1, 2>>, 2, 4)

    assert <<1, 2, 3, 4, 5, 6, 7, 8>> =
      strip_headers(<<0, 1, 2, 3, 0, 4, 5, 6, 0, 7, 8>>, 0, 4)

    assert <<1, 2, 3, 4, 5, 6, 7, 8>> =
      strip_headers(<<1, 2, 3, 0, 4, 5, 6, 0, 7, 8>>, 1, 4)
  end

  test "add_headers/3 adds the block headers" do
    assert <<0, 1, 2, 3>> =
      add_headers(<<1, 2, 3>>, 0, 4)

    assert <<1, 2>> =
      add_headers(<<1, 2>>, 2, 4)

    assert <<0, 1, 2, 3, 0, 4, 5, 6, 0, 7, 8>> =
      add_headers(<<1, 2, 3, 4, 5, 6, 7, 8>>, 0, 4)

    assert <<1, 2, 3, 0, 4, 5, 6, 0, 7, 8>> =
      add_headers(<<1, 2, 3, 4, 5, 6, 7, 8>>, 1, 4)
  end
end
