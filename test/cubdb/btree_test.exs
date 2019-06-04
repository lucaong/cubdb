defmodule CubDB.BtreeTest do
  use ExUnit.Case

  alias CubDB.Store
  alias Store.Utils
  alias CubDB.Btree
  doctest Btree

  @leaf Btree.__leaf__
  @branch Btree.__branch__

  def btree() do
    store = Store.MemMap.new
    root = Utils.load(store, {:Btree, 0, Btree.leaf()})
    %Btree{root: root, capacity: 3, store: store, size: 0}
  end

  def btree(root = {@leaf, cs}) do
    store = Store.MemMap.new
    root = Utils.load(store, {:Btree, length(cs), root})
    %Btree{root: root, capacity: 3, store: store, size: length(cs)}
  end

  def btree(root = {@branch, _}, size \\ 0) do
    store = Store.MemMap.new
    root = Utils.load(store, {:Btree, size, root})
    %Btree{root: root, capacity: 3, store: store, size: size}
  end

  test "insert/3 called on non-full leaf inserts the key/value tuple" do
    tree = Btree.insert(btree(), :foo, 1)
    assert {:Btree, 1, {@leaf, [foo: 1]}} = Utils.debug(tree.store)

    tree = Btree.insert(tree, :bar, 2)
    assert {:Btree, 2,
      {@leaf, [bar: 2, foo: 1]}
    } = Utils.debug(tree.store)
    tree = Btree.insert(tree, :baz, 3)
    assert {:Btree, 3,
      {@leaf, [bar: 2, baz: 3, foo: 1]}
    } = Utils.debug(tree.store)
    tree = Btree.insert(tree, :baz, 4)
    assert {:Btree, 3,
      {@leaf, [bar: 2, baz: 4, foo: 1]}
    } = Utils.debug(tree.store)
  end

  test "insert/3 called on full leaf splits it when overflowing" do
    tree = btree({@leaf, [bar: 2, baz: 3, foo: 1]})
    tree = Btree.insert(tree, :qux, 4)
    assert {:Btree, 4,
      {
        @branch, [
          bar: {@leaf, [bar: 2, baz: 3]},
          foo: {@leaf, [foo: 1, qux: 4]}
        ]
      }
    } = Utils.debug(tree.store)
  end

  test "insert/3 called on a branch inserts the key/value" do
    tree = btree({
      @branch, [
        bar: {@leaf, [bar: 2, baz: 3]},
        foo: {@leaf, [foo: 1, qux: 4]}
      ]
    })
    assert {:Btree, _,
      {
        @branch, [
          abc: {@leaf, [abc: 5, bar: 2, baz: 3]},
          foo: {@leaf, [foo: 1, qux: 4]}
        ]
      }
    } = Utils.debug(Btree.insert(tree, :abc, 5).store)
  end

  test "insert/3 called on a branch splits the branch if necessary" do
    tree = btree({
      @branch, [
        bar: {@leaf, [bar: 2, baz: 3]},
        foo: {@leaf, [foo: 1, quux: 5, qux: 4]},
        xxx: {@leaf, [xxx: 6, yyy: 7]}
      ]
    })
    assert {:Btree, _,
      {
        @branch, [
          bar: {
            @branch, [
              bar: {@leaf, [bar: 2, baz: 3]},
              foo: {@leaf, [foo: 1, quuux: 8]}
            ]
          },
          quux: {
            @branch, [
              quux: {@leaf, [quux: 5, qux: 4]},
              xxx: {@leaf, [xxx: 6, yyy: 7]}
            ]
          }
        ]
      }
    } = Utils.debug(Btree.insert(tree, :quuux, 8).store)
  end

  test "insert/3 increments the size of the tree only when necessary" do
    tree = btree()
    assert %Btree{size: 0} = tree
    tree = Btree.insert(tree, :foo, 1)
    assert %Btree{size: 1} = tree
    tree = Btree.insert(tree, :foo, 2)
    assert %Btree{size: 1} = tree
  end

  test "lookup/2 finds key and returns its value" do
    tiny_tree = btree({@leaf, [bar: 2, foo: 1]})
    assert 1 = Btree.lookup(tiny_tree, :foo)
    assert nil == Btree.lookup(tiny_tree, :non_existing)

    big_tree = btree({
      @branch, [
        bar: {
          @branch, [
            bar: {@leaf, [bar: 2, baz: 3]},
            foo: {@leaf, [foo: 1, quuux: 8]}
          ]
        },
        quux: {
          @branch, [
            quux: {@leaf, [quux: 5, qux: 4]},
            xxx: {@leaf, [xxx: 6, yyy: 7]}
          ]
        }
      ]
    })
    assert 1 = Btree.lookup(big_tree, :foo)
    assert nil == Btree.lookup(big_tree, :non_existing)
  end

  test "delete/2 removes a key/value" do
    tiny_tree = btree({@leaf, [bar: 2, foo: 1]})
    assert {:Btree, _,
      {@leaf, [bar: 2]}
    } = Utils.debug(Btree.delete(tiny_tree, :foo).store)
    assert tiny_tree == Btree.delete(tiny_tree, :non_existing)

    big_tree = btree({
      @branch, [
        bar: {@leaf, [bar: 2, baz: 3]},
        foo: {@leaf, [foo: 1, fox: 5, qux: 4]}
      ]
    })
    assert {:Btree, _,
      {
        @branch, [
          bar: {@leaf, [bar: 2, baz: 3]},
          fox: {@leaf, [fox: 5, qux: 4]}
        ]
      }
    } = Utils.debug(Btree.delete(big_tree, :foo).store)
    assert big_tree == Btree.delete(big_tree, :non_existing)
  end

  test "delete/2 merges nodes if necessary" do
    big_tree = btree({
      @branch, [
        bar: {
          @branch, [
            bar: {@leaf, [bar: 2, baz: 3]},
            foo: {@leaf, [foo: 1, quuux: 8]}
          ]
        },
        quux: {
          @branch, [
            quux: {@leaf, [quux: 5, qux: 4]},
            xxx: {@leaf, [xxx: 6, yyy: 7]}
          ]
        }
      ]
    })
    assert {:Btree, _,
      {
        @branch, [
          bar: {@leaf, [bar: 2, baz: 3]},
          foo: {@leaf, [foo: 1, quuux: 8]},
          quux: {@leaf, [quux: 5, qux: 4, yyy: 7]},
        ]
      }
    } = Utils.debug(Btree.delete(big_tree, :xxx).store)
  end

  test "delete/2 removes a node when empty" do
    tree = btree({
      @branch, [
        bar: {@leaf, [bar: 2]},
        foo: {@leaf, [foo: 1]}
      ]
    })

    tree = Btree.delete(tree, :bar)
    assert {:Btree, _,
      {@leaf, [foo: 1]}
    } = Utils.debug(tree.store)

    tree = Btree.delete(tree, :foo)
    assert {:Btree, _,
      {@leaf, []}
    } = Utils.debug(tree.store)
  end

  test "delete/2 decrements the size of the tree only when necessary" do
    store = Store.MemMap.new
    tree = Btree.new(store, [foo: 1, bar: 2, baz: 3, qux: 4], 3)
    assert %Btree{size: 4} = tree
    tree = Btree.delete(tree, :bar)
    assert %Btree{size: 3} = tree
    tree = Btree.delete(tree, :bar)
    assert %Btree{size: 3} = tree
  end

  test "load/3 creates a Btree from a sorted enumerable of key/values" do
    store = Store.MemMap.new
    key_vals = Stream.map((0..20), &({&1, &1}))
    tree = key_vals |> Btree.load(store, 4)
    assert key_vals |> Enum.to_list == tree |> Enum.to_list
  end

  test "Btree implements Enumerable" do
    Protocol.assert_impl!(Enumerable, Btree)

    empty_list  = []
    tiny_list   = [foo: 1, bar: 2, baz: 3]
    larger_list = [foo: 1, bar: 2, baz: 3, qux: 4, quux: 5,
                   xxx: 6, yyy: 7, quuux: 8]

    for elems <- [empty_list, tiny_list, larger_list] do
      sorted_elems = elems |> List.keysort(0)

      store = Store.MemMap.new
      tree = Btree.new(store, elems, 3)

      assert Enum.count(tree) == length(elems)
      assert Enum.into(tree, []) == sorted_elems
      assert Stream.map(tree, &(&1)) |> Enum.to_list == sorted_elems
      assert Stream.zip(tree, elems) |> Enum.to_list ==
        Enum.zip(sorted_elems, elems)

      if length(elems) > 0 do
        assert Enum.member?(tree, List.first(elems))
        assert Enum.member?(tree, {:not_there, nil}) == false
        assert Enum.member?(tree, :not_a_key_value_tuple) == false
      end
    end
  end
end
