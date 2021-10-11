defmodule CubDB.BtreeTest do
  use ExUnit.Case, async: true

  alias CubDB.Btree
  alias CubDB.Store
  alias TestHelper.Btree.Utils

  import TestHelper

  doctest Btree

  @leaf Btree.__leaf__()
  @branch Btree.__branch__()
  @deleted Btree.__deleted__()

  def compose_btree do
    {:ok, store} = Store.TestStore.create()
    {root_loc, root} = Utils.load(store, {:Btree, 0, Btree.leaf()})
    %Btree{root: root, root_loc: root_loc, capacity: 3, store: store, size: 0}
  end

  def compose_btree(root = {@leaf, cs}) do
    {:ok, store} = Store.TestStore.create()
    {root_loc, root} = Utils.load(store, {:Btree, length(cs), root})
    %Btree{root: root, root_loc: root_loc, capacity: 3, store: store, size: length(cs)}
  end

  def compose_btree(root = {@branch, _}, size \\ 0) do
    {:ok, store} = Store.TestStore.create()
    {root_loc, root} = Utils.load(store, {:Btree, size, root})
    %Btree{root: root, root_loc: root_loc, capacity: 3, store: store, size: size}
  end

  test "insert/3 called on non-full leaf inserts the key/value tuple" do
    tree = Btree.insert(compose_btree(), :foo, 1) |> Btree.commit()
    assert {:Btree, 1, {@leaf, [foo: 1]}} = Utils.debug(tree.store)

    tree = Btree.insert(tree, :bar, 2) |> Btree.commit()
    assert {:Btree, 2, {@leaf, [bar: 2, foo: 1]}} = Utils.debug(tree.store)
    tree = Btree.insert(tree, :baz, 3) |> Btree.commit()
    assert {:Btree, 3, {@leaf, [bar: 2, baz: 3, foo: 1]}} = Utils.debug(tree.store)
    tree = Btree.insert(tree, :baz, 4) |> Btree.commit()
    assert {:Btree, 3, {@leaf, [bar: 2, baz: 4, foo: 1]}} = Utils.debug(tree.store)
  end

  test "insert/3 called on full leaf splits it when overflowing" do
    tree = compose_btree({@leaf, [bar: 2, baz: 3, foo: 1]})
    tree = Btree.insert(tree, :qux, 4) |> Btree.commit()

    assert {:Btree, 4,
            {
              @branch,
              [
                bar: {@leaf, [bar: 2, baz: 3]},
                foo: {@leaf, [foo: 1, qux: 4]}
              ]
            }} = Utils.debug(tree.store)
  end

  test "insert/3 called on a branch inserts the key/value" do
    btree =
      compose_btree({
        @branch,
        [
          bar: {@leaf, [bar: 2, baz: 3]},
          foo: {@leaf, [foo: 1, qux: 4]}
        ]
      })
      |> Btree.insert(:abc, 5)
      |> Btree.commit()

    assert {:Btree, _,
            {
              @branch,
              [
                abc: {@leaf, [abc: 5, bar: 2, baz: 3]},
                foo: {@leaf, [foo: 1, qux: 4]}
              ]
            }} = Utils.debug(btree.store)
  end

  test "insert/3 called on a branch splits the branch if necessary" do
    btree =
      compose_btree({
        @branch,
        [
          bar: {@leaf, [bar: 2, baz: 3]},
          foo: {@leaf, [foo: 1, quux: 5, qux: 4]},
          xxx: {@leaf, [xxx: 6, yyy: 7]}
        ]
      })

    btree = Btree.insert(btree, :quuux, 8) |> Btree.commit()

    assert {:Btree, _,
            {
              @branch,
              [
                bar: {
                  @branch,
                  [
                    bar: {@leaf, [bar: 2, baz: 3]},
                    foo: {@leaf, [foo: 1, quuux: 8]}
                  ]
                },
                quux: {
                  @branch,
                  [
                    quux: {@leaf, [quux: 5, qux: 4]},
                    xxx: {@leaf, [xxx: 6, yyy: 7]}
                  ]
                }
              ]
            }} = Utils.debug(btree.store)
  end

  test "insert/3 increments the size of the tree only when necessary" do
    tree = compose_btree()
    assert %Btree{size: 0} = tree
    tree = Btree.insert(tree, :foo, 1) |> Btree.commit()
    assert %Btree{size: 1} = tree
    tree = Btree.insert(tree, :foo, 2) |> Btree.commit()
    assert %Btree{size: 1} = tree
    tree = Btree.mark_deleted(tree, :foo) |> Btree.commit()
    assert %Btree{size: 0} = tree
    tree = Btree.insert(tree, :foo, 1) |> Btree.commit()
    assert %Btree{size: 1} = tree
  end

  test "insert/3 increases dirt by one" do
    tree = compose_btree()
    assert %Btree{dirt: 0} = tree
    tree = Btree.insert(tree, :foo, 1) |> Btree.commit()
    assert %Btree{dirt: 1} = tree
    tree = Btree.insert(tree, :foo, 2) |> Btree.commit()
    assert %Btree{dirt: 2} = tree
  end

  test "insert/3 does not write the header" do
    tree = Btree.insert(compose_btree(), :foo, 1)

    assert {:Btree, 0, {@leaf, []}} = Utils.debug(tree.store)

    tree = Btree.commit(tree)

    assert {:Btree, 1, {@leaf, [foo: 1]}} = Utils.debug(tree.store)
  end

  test "insert_new/3 does not overwrite existing entries" do
    btree =
      compose_btree({
        @branch,
        [
          bar: {@leaf, [bar: 2, baz: 3]},
          foo: {@leaf, [foo: 1, quux: 5, qux: 4]},
          xxx: {@leaf, [xxx: 6, yyy: @deleted, zzz: 7]}
        ]
      })

    assert {:error, :exists} = Btree.insert_new(btree, :bar, 123)
    assert {:error, :exists} = Btree.insert_new(btree, :baz, 123)
    assert {:error, :exists} = Btree.insert_new(btree, :quux, 123)
    assert ^btree = Btree.delete(btree, :quuux)

    btree = Btree.insert_new(btree, :quuux, 123)
    assert btree != {:error, :exists}

    btree = Btree.insert_new(btree, :yyy, 321)
    assert btree != {:error, :exists}

    assert [bar: 2, baz: 3, foo: 1, quuux: 123, quux: 5, qux: 4, xxx: 6, yyy: 321, zzz: 7] =
             btree |> Enum.into([])
  end

  test "fetch/2 finds key and returns {:ok, value} or :error" do
    tiny_tree = compose_btree({@leaf, [bar: 2, foo: 1]})
    assert {:ok, 1} = Btree.fetch(tiny_tree, :foo)
    assert :error == Btree.fetch(tiny_tree, :non_existing)

    big_tree =
      compose_btree({
        @branch,
        [
          bar: {
            @branch,
            [
              bar: {@leaf, [bar: 2, baz: 3]},
              foo: {@leaf, [foo: 1, quuux: 8]}
            ]
          },
          quux: {
            @branch,
            [
              quux: {@leaf, [quux: 5, qux: 4]},
              xxx: {@leaf, [xxx: 6, yyy: 7]}
            ]
          }
        ]
      })

    assert {:ok, 1} = Btree.fetch(big_tree, :foo)
    assert :error == Btree.fetch(big_tree, :non_existing)
  end

  test "delete/2 removes a key/value" do
    tiny_tree = compose_btree({@leaf, [bar: 2, foo: 1]})
    btree = Btree.delete(tiny_tree, :foo) |> Btree.commit()

    assert {:Btree, _, {@leaf, [bar: 2]}} = Utils.debug(btree.store)
    assert tiny_tree == Btree.delete(tiny_tree, :non_existing) |> Btree.commit()

    big_tree =
      compose_btree({
        @branch,
        [
          bar: {@leaf, [bar: 2, baz: 3]},
          foo: {@leaf, [foo: 1, fox: 5, qux: 4]}
        ]
      })

    btree = Btree.delete(big_tree, :foo) |> Btree.commit()

    assert {:Btree, _,
            {
              @branch,
              [
                bar: {@leaf, [bar: 2, baz: 3]},
                fox: {@leaf, [fox: 5, qux: 4]}
              ]
            }} = Utils.debug(btree.store)

    assert big_tree == Btree.delete(big_tree, :non_existing) |> Btree.commit()
  end

  test "delete/2 merges nodes if necessary" do
    big_tree =
      compose_btree({
        @branch,
        [
          bar: {
            @branch,
            [
              bar: {@leaf, [bar: 2, baz: 3]},
              foo: {@leaf, [foo: 1, quuux: 8]}
            ]
          },
          quux: {
            @branch,
            [
              quux: {@leaf, [quux: 5, qux: 4]},
              xxx: {@leaf, [xxx: 6, yyy: 7]}
            ]
          }
        ]
      })

    btree = Btree.delete(big_tree, :xxx) |> Btree.commit()

    assert {:Btree, _,
            {
              @branch,
              [
                bar: {@leaf, [bar: 2, baz: 3]},
                foo: {@leaf, [foo: 1, quuux: 8]},
                quux: {@leaf, [quux: 5, qux: 4, yyy: 7]}
              ]
            }} = Utils.debug(btree.store)
  end

  test "delete/2 removes a node when empty" do
    tree =
      compose_btree({
        @branch,
        [
          bar: {@leaf, [bar: 2]},
          foo: {@leaf, [foo: 1]}
        ]
      })

    tree = Btree.delete(tree, :bar) |> Btree.commit()
    assert {:Btree, _, {@leaf, [foo: 1]}} = Utils.debug(tree.store)

    tree = Btree.delete(tree, :foo) |> Btree.commit()
    assert {:Btree, _, {@leaf, []}} = Utils.debug(tree.store)
  end

  test "delete/2 decrements the size of the tree only when necessary" do
    {:ok, store} = Store.TestStore.create()
    tree = make_btree(store, [foo: 1, bar: 2, baz: 3, qux: 4], 3)
    assert %Btree{size: 4} = tree
    tree = Btree.delete(tree, :bar) |> Btree.commit()
    assert %Btree{size: 3} = tree
    tree = Btree.delete(tree, :bar) |> Btree.commit()
    assert %Btree{size: 3} = tree
  end

  test "delete/2 increases dirt by one when removing an entry" do
    {:ok, store} = Store.TestStore.create()
    tree = make_btree(store, [foo: 1, bar: 2, baz: 3, qux: 4], 3)
    assert %Btree{dirt: 4} = tree
    tree = Btree.delete(tree, :foo) |> Btree.commit()
    assert %Btree{dirt: 5} = tree
    tree = Btree.delete(tree, :foo) |> Btree.commit()
    assert %Btree{dirt: 5} = tree
  end

  test "delete/2 does not write the header" do
    tree = Btree.delete(compose_btree({@leaf, [foo: 1]}), :foo)

    assert {:Btree, 1, {@leaf, [foo: 1]}} = Utils.debug(tree.store)

    tree = Btree.commit(tree)

    assert {:Btree, 0, {@leaf, []}} = Utils.debug(tree.store)
  end

  test "mark_deleted/2 removes an entry" do
    {:ok, store} = Store.TestStore.create()

    btree =
      make_btree(store, [foo: 1, bar: 2, baz: 3, qux: 4], 3)
      |> Btree.mark_deleted(:bar)
      |> Btree.commit()

    assert Btree.fetch(btree, :bar) == :error
  end

  test "mark_deleted/2 decrements the size of the tree only when necessary" do
    {:ok, store} = Store.TestStore.create()
    tree = make_btree(store, [foo: 1, bar: 2, baz: 3, qux: 4], 3)
    assert %Btree{size: 4} = tree
    tree = Btree.mark_deleted(tree, :bar) |> Btree.commit()
    assert %Btree{size: 3} = tree
    tree = Btree.mark_deleted(tree, :bar) |> Btree.commit()
    assert %Btree{size: 3} = tree
    tree = Btree.delete(tree, :bar) |> Btree.commit()
    assert %Btree{size: 3} = tree
    tree = Btree.mark_deleted(tree, :bar) |> Btree.commit()
    assert %Btree{size: 3} = tree
  end

  test "mark_deleted/2 increases dirt by one only when necessary" do
    {:ok, store} = Store.TestStore.create()
    tree = make_btree(store, [foo: 1, bar: 2, baz: 3, qux: 4], 3)
    assert %Btree{dirt: 4} = tree
    tree = Btree.mark_deleted(tree, :bar) |> Btree.commit()
    assert %Btree{dirt: 5} = tree
    tree = Btree.mark_deleted(tree, :bar) |> Btree.commit()
    assert %Btree{dirt: 5} = tree
  end

  test "mark_deleted/2 does not write the header" do
    tree = Btree.mark_deleted(compose_btree({@leaf, [foo: 1]}), :foo)

    assert {:Btree, 1, {@leaf, [foo: 1]}} = Utils.debug(tree.store)

    tree = Btree.commit(tree)

    assert {:Btree, 0, {@leaf, [foo: @deleted]}} = Utils.debug(tree.store)
  end

  test "clear/1 deletes all entries" do
    tree = compose_btree({@leaf, [foo: 1, bar: 2, baz: 3]})
    tree = Btree.clear(tree) |> Btree.commit()

    assert {:Btree, 0, {@leaf, []}} = Utils.debug(tree.store)
  end

  test "clear/1 does not write the header" do
    tree = Btree.clear(compose_btree({@leaf, [foo: 1]}))

    assert {:Btree, 1, {@leaf, [foo: 1]}} = Utils.debug(tree.store)

    tree = Btree.commit(tree)

    assert {:Btree, 0, {@leaf, []}} = Utils.debug(tree.store)
  end

  test "clear/1 result in a size of 0 but increases the dirt" do
    {:ok, store} = Store.TestStore.create()
    tree = make_btree(store, [foo: 1, bar: 2, baz: 3, qux: 4], 3)
    %Btree{dirt: original_dirt} = tree

    tree = Btree.clear(tree) |> Btree.commit()
    expected_dirt = original_dirt + 1
    assert %Btree{size: 0, dirt: ^expected_dirt} = tree
  end

  test "load/3 creates a Btree from a sorted enumerable of key/values" do
    {:ok, store} = Store.TestStore.create()
    key_vals = Stream.map(0..19, &{&1, &1})
    tree = key_vals |> Btree.load(store, 4)
    assert key_vals |> Enum.to_list() == tree |> Enum.to_list()
  end

  test "load/3 creates a Btree from a single item Enumerable" do
    {:ok, store} = Store.TestStore.create()
    key_vals = [foo: 123]
    tree = key_vals |> Btree.load(store, 4)
    assert ^key_vals = tree |> Enum.to_list()
  end

  test "load/3 creates a Btree from an empty Enumerable" do
    {:ok, store} = Store.TestStore.create()
    key_vals = []
    tree = key_vals |> Btree.load(store, 4)
    assert [] = tree |> Enum.to_list()
  end

  test "load/3 raises ArgumentError if the given store is not empty" do
    {:ok, store} = Store.TestStore.create()
    key_vals = Stream.map(0..20, &{&1, &1})
    key_vals |> Btree.load(store, 4)

    assert_raise ArgumentError, fn ->
      key_vals |> Btree.load(store, 4)
    end
  end

  test "load/3 sets dirt to 0" do
    {:ok, store} = Store.TestStore.create()
    key_vals = Stream.map(0..20, &{&1, &1})
    tree = key_vals |> Btree.load(store, 4)
    assert %Btree{dirt: 0} = tree

    {:ok, store} = Store.TestStore.create()
    key_vals = [foo: 123]
    tree = key_vals |> Btree.load(store, 4)
    assert %Btree{dirt: 0} = tree
  end

  test "key_range/3 returns a KeyRange" do
    {:ok, store} = Store.TestStore.create()
    btree = make_btree(store, a: 1, b: 2, c: 3, d: 4, e: 5)
    min_key = :b
    max_key = :e
    reverse = true

    assert %Btree.KeyRange{
             btree: ^btree,
             min_key: {^min_key, true},
             max_key: {^max_key, true},
             reverse: ^reverse
           } = Btree.key_range(btree, {min_key, true}, {max_key, true}, reverse)

    assert %Btree.KeyRange{
             btree: ^btree,
             min_key: nil,
             max_key: {^max_key, false},
             reverse: ^reverse
           } = Btree.key_range(btree, nil, {max_key, false}, reverse)

    assert %Btree.KeyRange{
             btree: ^btree,
             min_key: {^min_key, false},
             max_key: nil,
             reverse: false
           } = Btree.key_range(btree, {min_key, false}, nil)
  end

  test "dirt_factor/1 returns a numeric dirt factor" do
    {:ok, store} = Store.TestStore.create()
    btree = Btree.new(store)

    assert Btree.dirt_factor(btree) == 0

    btree = Btree.insert(btree, :foo, 1)
    assert Btree.dirt_factor(btree) == 1 / 3

    btree = Btree.delete(btree, :foo)
    assert Btree.dirt_factor(btree) == 2 / 3

    btree = Btree.clear(btree)
    assert Btree.dirt_factor(btree) == 3 / 4
  end

  test "Btree implements Enumerable" do
    Protocol.assert_impl!(Enumerable, Btree)

    empty_list = []
    tiny_list = [foo: 1, bar: 2, baz: 3]
    larger_list = [foo: 1, bar: 2, baz: 3, qux: 4, quux: 5, xxx: 6, yyy: 7, quuux: 8]

    for elems <- [empty_list, tiny_list, larger_list] do
      sorted_elems = elems |> List.keysort(0)

      {:ok, store} = Store.TestStore.create()
      tree = make_btree(store, elems, 3)

      assert Enum.count(tree) == length(elems)
      assert Enum.into(tree, []) == sorted_elems
      assert Stream.map(tree, & &1) |> Enum.to_list() == sorted_elems

      assert Stream.zip(tree, elems) |> Enum.to_list() ==
               Enum.zip(sorted_elems, elems)

      if length(elems) > 0 do
        assert Enum.member?(tree, List.first(elems))
        assert Enum.member?(tree, {:not_there, nil}) == false
        assert Enum.member?(tree, :not_a_key_value_tuple) == false
      end
    end
  end

  test "Enumerable.Btree.reduce/3 skips nodes marked as deleted" do
    {:ok, store} = Store.TestStore.create()
    tree = make_btree(store, [a: 1, b: 2, c: 3, d: 4], 3) |> Btree.mark_deleted(:b)
    assert Enum.to_list(tree) == [a: 1, c: 3, d: 4]
  end

  test "Btree.leaf creates a leaf node" do
    leaf_marker = Btree.__leaf__()
    assert {^leaf_marker, []} = Btree.leaf()
    assert {^leaf_marker, [1, 2, 3]} = Btree.leaf(children: [1, 2, 3])
  end

  test "Btree.branch creates a branch node" do
    branch_marker = Btree.__branch__()
    assert {^branch_marker, []} = Btree.branch()
    assert {^branch_marker, [1, 2, 3]} = Btree.branch(children: [1, 2, 3])
  end

  test "Btree.value creates a value node" do
    value_marker = Btree.__value__()
    assert {^value_marker, nil} = Btree.value()
    assert {^value_marker, "hello"} = Btree.value(val: "hello")
  end

  test "Btree.alive? returns true if the Store is open, otherwise false" do
    {:ok, store} = Store.TestStore.create()
    btree = Btree.new(store)
    assert Btree.alive?(btree) == true

    Store.close(store)
    assert Btree.alive?(btree) == false
  end
end
