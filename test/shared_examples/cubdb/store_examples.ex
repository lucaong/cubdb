defmodule CubDB.StoreExamples do
  @moduledoc false

  import SharedExamples

  shared_examples do
    alias CubDB.Btree

    import Btree, only: [header: 1, value: 1]

    test "put_node/2 and get_node/2 set and get a node at location", %{store: store} do
      Enum.each(1..10, fn value ->
        node = value(val: value)
        loc = CubDB.Store.put_node(store, node)
        assert node == CubDB.Store.get_node(store, loc)
      end)
    end

    test "get_node/2 raises an error if no node is found at given location", %{store: store} do
      assert_raise ArgumentError, fn ->
        CubDB.Store.get_node(store, 42)
      end
    end

    test "put_header/2 sets a header", %{store: store} do
      root_loc = CubDB.Store.put_node(store, value(val: 1))

      loc =
        CubDB.Store.put_header(store, header(size: 1, location: root_loc, dirt: 0, metadata: []))

      assert {^loc, header(size: 1, location: ^root_loc, dirt: 0, metadata: [])} =
               CubDB.Store.get_latest_header(store)
    end

    test "get_latest_header/1 returns the most recently stored header", %{store: store} do
      CubDB.Store.put_node(store, value(val: 1))
      CubDB.Store.put_node(store, value(val: 2))
      CubDB.Store.put_header(store, header(size: 0, location: 0, dirt: 0, metadata: []))
      CubDB.Store.put_node(store, value(val: 3))
      loc = CubDB.Store.put_header(store, header(size: 42, location: 0, dirt: 0, metadata: []))
      CubDB.Store.put_node(store, value(val: 4))

      assert {^loc, header(size: 42, location: 0, dirt: 0, metadata: [])} =
               CubDB.Store.get_latest_header(store)
    end

    test "blank?/1 returns true if store is blank, and false otherwise", %{store: store} do
      assert CubDB.Store.blank?(store) == true
      CubDB.Store.put_node(store, value(val: 1))
      CubDB.Store.put_header(store, header(size: 0, location: 0, dirt: 0, metadata: []))
      CubDB.Store.sync(store)
      assert CubDB.Store.blank?(store) == false
    end

    test "close/1 returns :ok", %{store: store} do
      assert CubDB.Store.close(store) == :ok
    end
  end
end
