defmodule CubDB.Btree do
  require Record
  Record.defrecord :leaf, :Leaf, children: []
  Record.defrecord :branch, :Branch, children: []

  alias CubDB.Store
  alias CubDB.Btree

  @default_capacity 32
  @enforce_keys [:root, :size, :store]
  defstruct root: nil, size: 0, store: nil, capacity: @default_capacity

  def new(store) do
    new(store, @default_capacity)
  end

  def new(store, cap) when is_integer(cap) do
    case Store.get_latest_header(store) do
      {_, {s, loc}} ->
        %Btree{root: loc, size: s, capacity: cap, store: store}
      nil ->
        loc = Store.put_node(store, leaf())
        Store.put_header(store, {0, loc})
        %Btree{root: loc, size: 0, capacity: cap, store: store}
    end
  end

  def new(store, elems, cap \\ @default_capacity) when is_list(elems) do
    Enum.reduce(elems, new(store, cap), fn {k, v}, tree ->
      Btree.insert(tree, k, v)
    end)
  end

  def lookup(%Btree{root: root_loc, store: store}, key) do
    root = Store.get_node(store, root_loc)
    {{:Leaf, children}, _} = lookup_leaf(root, store, key, [])
    with {_, loc} <- Enum.find(children, &match?({^key, _}, &1)) do
      {:Value, value} = Store.get_node(store, loc)
      value
    end
  end

  def insert(%Btree{root: root_loc, store: store, capacity: cap, size: s}, key, value) do
    root = Store.get_node(store, root_loc)
    {leaf = {:Leaf, children}, path} = lookup_leaf(root, store, key, [])
    new_root = build_up(store, leaf, [{key, {:Value, value}}], [], path, cap)
    s = if List.keymember?(children, key, 0), do: s, else: s + 1
    Store.put_header(store, {s, new_root})
    %Btree{root: new_root, capacity: cap, store: store, size: s}
  end

  def delete(btree = %Btree{root: root_loc, store: store, capacity: cap, size: s}, key) do
    root = Store.get_node(store, root_loc)
    {leaf = {:Leaf, children}, path} = lookup_leaf(root, store, key, [])
    if List.keymember?(children, key, 0) do
      new_root = build_up(store, leaf, [], [key], path, cap)
      Store.put_header(store, {s - 1, new_root})
      %Btree{root: new_root, capacity: cap, store: store, size: s - 1}
    else
      btree
    end
  end

  def commit(%Btree{store: store}) do
    Store.commit(store)
  end

  defp lookup_leaf(branch = {:Branch, children}, store, key, path) do
    child = Enum.reduce_while(children, nil, fn
      ({_, loc}, nil) -> {:cont, Store.get_node(store, loc)}
      ({k, loc}, acc) ->
        if k <= key, do: {:cont, Store.get_node(store, loc)}, else: {:halt, acc}
    end)
    lookup_leaf(child, store, key, [branch | path])
  end

  defp lookup_leaf(leaf = {:Leaf, _}, _, _, path) do
    {leaf, path}
  end

  defp build_up(store, node, to_merge, to_delete, [], cap) do
    to_merge_locs = store_nodes(store, to_merge)
    case replace_node(store, node, to_merge_locs, to_delete, nil, cap) do
      [] -> Store.put_node(store, leaf())
      [{_, {:Branch, [{_, loc}]}}] -> loc
      [{_, node}] -> Store.put_node(store, node)
      new_nodes ->
        new_locs = store_nodes(store, new_nodes)
        Store.put_node(store, {:Branch, new_locs})
    end
  end

  defp build_up(store, node, to_merge, to_delete, [parent | up], cap) do
    to_merge_locs = store_nodes(store, to_merge)
    new_nodes = replace_node(store, node, to_merge_locs, to_delete, parent, cap)
    deleted = keys(elem(node, 1)) -- keys(new_nodes)
    build_up(store, parent, new_nodes, deleted, up, cap)
  end

  defp store_nodes(store, nodes) do
    Enum.map(nodes, fn {k, v} ->
      {k, Store.put_node(store, v)}
    end)
  end

  defp replace_node(store, node, merge, delete, parent, cap) do
    {type, children} = node
    children
    |> update_children(merge, delete)
    |> split_merge(store, node, parent, cap)
    |> wrap_nodes(type)
  end

  defp update_children(children, merge, delete) do
    merged = Enum.reduce(merge, children, fn kv = {k, _}, acc ->
      List.keystore(acc, k, 0, kv)
    end)
    Enum.reduce(delete, merged, fn k, acc ->
      List.keydelete(acc, k, 0)
    end) |> List.keysort(0)
  end

  defp wrap_nodes(chunks, type) do
    for chunk = [{k, _} | _] <- chunks do
      {k, {type, chunk}}
    end
  end

  defp split_merge(children, store, old_node, parent, cap) do
    size = length(children)
    cond do
      size > cap -> split(children, cap)
      size < div(cap + 1, 2) and parent != nil and old_node != nil ->
        merge(store, children, old_node, parent, cap)
      true -> [children]
    end
  end

  defp split(children, cap) do
    children
    |> Enum.split(div(cap + 1, 2))
    |> Tuple.to_list
  end

  defp merge(store, children, {_, old_children}, parent, cap) do
    key = min_key(keys(old_children), keys(children))
    left_sibling(store, parent, key) ++ children
    |> split_merge(store, nil, parent, cap)
  end

  defp left_sibling(store, {:Branch, children}, key) do
    left = children
           |> Enum.take_while(fn {k, _} -> k < key end)
           |> List.last
    case left do
      {_, loc} ->
        {_, children} = Store.get_node(store, loc)
        children
      nil -> []
    end
  end

  defp keys(tuples) do
    Enum.map(tuples, &(elem(&1, 0)))
  end

  defp min_key([], ks2), do: List.first(ks2)
  defp min_key(ks1, []), do: List.first(ks1)
  defp min_key(ks1, ks2), do: min(List.first(ks1), List.first(ks2))
end

defimpl Enumerable, for: CubDB.Btree do
  alias CubDB.Store
  alias CubDB.Btree

  def reduce(%Btree{root: root_loc, store: store}, cmd_acc, fun) do
    root = {_, locs} = Store.get_node(store, root_loc)
    children = Enum.map(locs, fn {k, v} ->
      {k, Store.get_node(store, v)}
    end)
    case root do
      {:Branch, _} -> do_reduce({[], [children]}, cmd_acc, fun, store)
      {:Leaf, _}   -> do_reduce({children, []}, cmd_acc, fun, store)
    end
  end

  def count(%Btree{size: size}), do: {:ok, size}

  def member?(btree, key) do
    Btree.lookup(btree, key) != nil
  end

  defp do_reduce(_, {:halt, acc}, _, _), do: {:halted, acc}

  defp do_reduce(t, {:suspend, acc}, fun, store) do
    {:suspended, acc, &do_reduce(t, &1, fun, store)}
  end

  defp do_reduce({[], []}, {:cont, acc}, _, _), do: {:done, acc}

  defp do_reduce(t, {:cont, acc}, fun, store) do
    case next(t, store) do
      {t, item} -> do_reduce(t, fun.(item, acc), fun, store)
      :done     -> {:done, acc}
    end
  end

  defp next({[], [[] | todo]}, store) do
    case todo do
      [] -> :done
      _ -> next({[], todo}, store)
    end
  end

  defp next({[], [[{_, {:Leaf, locs}} | rest] | todo]}, store) do
    children = Enum.map(locs, fn {k, v} ->
      {k, Store.get_node(store, v)}
    end)
    next({children, [rest | todo]}, store)
  end

  defp next({[], [[{_, {:Branch, locs}} | rest] | todo]}, store) do
    children = Enum.map(locs, fn {k, v} ->
      {k, Store.get_node(store, v)}
    end)
    next({[], [children | [rest | todo]]}, store)
  end

  defp next({[{k, {:Value, v}} | rest], todo}, _) do
    {{rest, todo}, {k, v}}
  end
end
