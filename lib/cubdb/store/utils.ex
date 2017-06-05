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
    {root_loc, root_node} = load_node(store, root)
    Store.put_header(store, {size, root_loc})
    root_node
  end

  defp load_node(store, {type, children}) do
    locs = Enum.map(children, fn {k, child} ->
      {loc, _} = load_node(store, child)
      {k, loc}
    end)
    node = {type, locs}
    {Store.put_node(store, node), node}
  end

  defp load_node(store, value) do
    node = {:Value, value}
    {Store.put_node(store, node), node}
  end
end
