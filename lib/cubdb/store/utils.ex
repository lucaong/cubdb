defmodule CubDB.Store.Utils do
  alias CubDB.Store

  def debug(store) do
    {_, {size, root_loc}} = Store.get_latest_header(store)
    {:Btree, size, debug_node(store, root_loc)}
  end

  defp debug_node(store, loc) do
    case Store.get_node(store, loc) do
      {:Value, value} -> value
      {type, locs} ->
        children = Enum.map(locs, fn {k, loc} ->
          {k, debug_node(store, loc)}
        end)
        {type, children}
    end
  end

  def load(store, {:Btree, size, root}) do
    root_loc = load_node(store, root)
    Store.put_node(store, {:Btree, size, root_loc})
    root_loc
  end

  defp load_node(store, {type, children}) do
    locs = Enum.map(children, fn {k, child} ->
      {k, load_node(store, child)}
    end)
    Store.put_node(store, {type, locs})
  end

  defp load_node(store, value) do
    Store.put_node(store, {:Value, value})
  end
end
