defmodule CubDB.Store.File.BlocksTest do
  use ExUnit.Case

  import CubDB.Store.File.Blocks

  test "length_with_markers/3 returns an integer" do
    assert is_integer(length_with_markers(0, 1, 4))
  end

  test "length_with_markers/3 computes length including block markers" do
    #  - -
    # |x|.| | |
    assert length_with_markers(0, 1, 4) == 2
    #    - -
    # |x|.|.|.|
    assert length_with_markers(1, 3, 4) == 3
    #  - - - - - - - - - - -
    # |x|.|.|.|x|.|.|.|x|.|.| |
    assert length_with_markers(0, 8, 4) == 11
    #    - - - - - - - - - -
    # |x|.|.|.|x|.|.|.|x|.|.| |
    assert length_with_markers(1, 8, 4) == 10
    #      - - - - - - - - - -
    # |x| |.|.|x|.|.|.|x|.|.|.|
    assert length_with_markers(2, 8, 4) == 10
    #        - - - - - - - - - - -
    # |x| | |.|x|.|.|.|x|.|.|.|x|.|
    assert length_with_markers(3, 8, 4) == 11
    #  - - - - - - - - - - - - - -
    # |x|.|.|.|x|.|.|.|x|.|.|.|x|.|
    assert length_with_markers(0, 10, 4) == 14
  end

  test "strip_markers/3 removes the block headers and returns an iolist" do
    assert [<<1, 2, 3>>] = strip_markers(<<0, 1, 2, 3>>, 0, 4)

    assert [<<1, 2>>] = strip_markers(<<1, 2>>, 2, 4)

    assert [<<1, 2, 3>>, <<4, 5, 6>>, <<7, 8>>] =
             strip_markers(<<0, 1, 2, 3, 0, 4, 5, 6, 0, 7, 8>>, 0, 4)

    assert [<<1, 2, 3>>, <<4, 5, 6>>, <<7, 8>>] =
             strip_markers(<<1, 2, 3, 0, 4, 5, 6, 0, 7, 8>>, 1, 4)
  end

  test "add_markers/3 adds the block headers and returns an iolist" do
    assert [<<0>>, <<1, 2, 3>>] = add_markers(<<1, 2, 3>>, 0, 4)

    assert [<<1, 2>>] = add_markers(<<1, 2>>, 2, 4)

    assert [<<0>>, <<1, 2, 3>>, <<0>>, <<4, 5, 6>>, <<0>>, <<7, 8>>] =
             add_markers(<<1, 2, 3, 4, 5, 6, 7, 8>>, 0, 4)

    assert [<<1, 2, 3>>, <<0>>, <<4, 5, 6>>, <<0>>, <<7, 8>>] =
             add_markers(<<1, 2, 3, 4, 5, 6, 7, 8>>, 1, 4)
  end

  test "add_header_marker/3 adds padding and header marker and returns an iolist" do
    assert {0, [<<42>>, <<1, 2, 3>>]} = add_header_marker(<<1, 2, 3>>, 0, 4)

    assert {4, [<<0, 0>>, <<42>>, <<1, 2, 3>>]} = add_header_marker(<<1, 2, 3>>, 2, 4)

    assert {4, [<<0, 0, 0>>, <<42>>, <<1, 2, 3>>]} = add_header_marker(<<1, 2, 3>>, 1, 4)

    assert {4, [<<0, 0>>, <<42>>, <<1, 2, 3>>, <<0>>, <<4, 5>>]} =
             add_header_marker(<<1, 2, 3, 4, 5>>, 2, 4)
  end

  test "latest_possible_header/2 returns latest possible header loc" do
    assert latest_possible_header(10, 4) == 8
    assert latest_possible_header(8, 4) == 4
    assert latest_possible_header(2, 4) == 0
  end
end
