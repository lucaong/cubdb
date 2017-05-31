defmodule CubDB.StoreExamples do
  use ExUnit.CaseTemplate

  using do
    quote do
      test "put_node/2 and get_node/2 set and get a node", %{store: store} do
        node = {:Value, 42}
        loc = CubDB.Store.put_node(store, node)
        assert node == CubDB.Store.get_node(store, loc)
      end
    end
  end
end

