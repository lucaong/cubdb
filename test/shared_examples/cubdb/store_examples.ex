defmodule CubDB.StoreExamples do
  use ExUnit.CaseTemplate

  using do
    quote do
      alias CubDB.Btree

      @value Btree.__value__

      test "put_node/2 and get_node/2 set and get a node at location", %{store: store} do
        Enum.each((1..10), fn value ->
          node = {@value, value}
          loc = CubDB.Store.put_node(store, node)
          assert node == CubDB.Store.get_node(store, loc)
        end)
      end

      test "get_node/2 returns error if no node is found at given location", %{store: store} do
        assert {:error, _} = CubDB.Store.get_node(store, 42)
      end

      test "put_header/2 sets a header", %{store: store} do
        root_loc = CubDB.Store.put_node(store, {@value, 1})
        loc = CubDB.Store.put_header(store, {root_loc, 1})
        assert {^loc, {^root_loc, 1}} =
          CubDB.Store.get_latest_header(store)
      end

      test "get_latest_header/1 returns the most recently stored header", %{store: store} do
        CubDB.Store.put_node(store, {@value, 1})
        CubDB.Store.put_node(store, {@value, 2})
        CubDB.Store.put_header(store, {0, 0})
        CubDB.Store.put_node(store, {@value, 3})
        loc = CubDB.Store.put_header(store, {42, 0})
        CubDB.Store.put_node(store, {@value, 4})
        assert {^loc, {42, 0}} = CubDB.Store.get_latest_header(store)
      end

      test "blank?/1 returns true if store is blank, and false otherwise", %{store: store} do
        assert CubDB.Store.blank?(store) == true
        CubDB.Store.put_node(store, {@value, 1})
        CubDB.Store.put_header(store, {0, 0})
        CubDB.Store.commit(store)
        assert CubDB.Store.blank?(store) == false
      end
    end
  end
end
