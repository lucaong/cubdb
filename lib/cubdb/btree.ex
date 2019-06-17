defmodule CubDB.Btree do
  @moduledoc false

  @leaf :l
  @branch :b
  @value :v
  @deleted :d

  require Record
  Record.defrecord(:leaf, @leaf, children: [])
  Record.defrecord(:branch, @branch, children: [])
  Record.defrecord(:value, @value, val: nil)

  @type key :: any
  @type val :: any
  @type btree_size :: non_neg_integer
  @type location :: non_neg_integer
  @type leaf_node :: record(:leaf, children: list({key, location}))
  @type branch_node :: record(:branch, children: list({key, location}))
  @type value_node :: record(:value, val: val)
  @type deleted_node :: :d
  @type terminal_node :: value_node | deleted_node
  @type btree_node :: leaf_node | branch_node | value_node | deleted_node
  @type btree_header :: {btree_size, location}

  alias CubDB.Store
  alias CubDB.Btree

  @type t :: %Btree{
          root: branch_node | leaf_node,
          root_loc: location,
          size: btree_size,
          store: Store.t(),
          capacity: non_neg_integer
        }

  @default_capacity 32
  @enforce_keys [:root, :root_loc, :size, :store, :capacity]
  defstruct root: nil, root_loc: nil, size: 0, store: nil, capacity: @default_capacity

  @spec new(Store.t(), pos_integer) :: Btree.t()

  def new(store, cap \\ @default_capacity) do
    case Store.get_latest_header(store) do
      {_, {s, loc}} ->
        root = Store.get_node(store, loc)
        %Btree{root: root, root_loc: loc, size: s, capacity: cap, store: store}

      nil ->
        root = leaf()
        loc = Store.put_node(store, root)
        Store.put_header(store, {0, loc})
        %Btree{root: root, root_loc: loc, size: 0, capacity: cap, store: store}
    end
  end

  @spec load(Enumerable.t(), Store.t(), pos_integer) :: Btree.t()

  def load(enum, store, cap \\ @default_capacity) do
    unless Store.blank?(store),
      do: raise(ArgumentError, message: "cannot load into non-empty store")

    {st, count} =
      Enum.reduce(enum, {[], 0}, fn {k, v}, {st, count} ->
        {load_node(store, k, {@value, v}, st, 1, cap), count + 1}
      end)

    if count == 0 do
      new(store, cap)
    else
      {root, root_loc} = finalize_load(store, st, 1, cap)
      Store.put_header(store, {count, root_loc})
      %Btree{root: root, root_loc: root_loc, capacity: cap, store: store, size: count}
    end
  end

  @spec lookup(Btree.t(), key) :: val | nil

  def lookup(tree = %Btree{}, key) do
    case has_key?(tree, key) do
      {true, value} -> value
      {false, _} -> nil
    end
  end

  @spec has_key?(Btree.t(), key) :: {true, val} | {false, nil}

  def has_key?(%Btree{root: root, store: store}, key) do
    {{@leaf, children}, _} = lookup_leaf(root, store, key, [])

    case Enum.find(children, &match?({^key, _}, &1)) do
      nil ->
        {false, nil}

      {_, loc} ->
        case Store.get_node(store, loc) do
          {@value, value} -> {true, value}
          @deleted -> {false, nil}
        end
    end
  end

  @spec insert(Btree.t(), key, val) :: Btree.t()

  def insert(btree, key, value) do
    insert_terminal_node(btree, key, {@value, value})
  end

  @spec delete(Btree.t(), key) :: Btree.t()

  def delete(btree = %Btree{root: root, store: store, capacity: cap, size: s}, key) do
    {leaf = {@leaf, children}, path} = lookup_leaf(root, store, key, [])

    case List.keyfind(children, key, 0) do
      {^key, loc} ->
        size =
          case Store.get_node(store, loc) do
            @deleted -> s
            _ -> s - 1
          end

        {root_loc, new_root} = build_up(store, leaf, [], [key], path, cap)
        Store.put_header(store, {size, root_loc})
        %Btree{root: new_root, root_loc: root_loc, capacity: cap, store: store, size: size}

      nil ->
        btree
    end
  end

  @spec mark_deleted(Btree.t(), key) :: Btree.t()

  def mark_deleted(btree, key) do
    case has_key?(btree, key) do
      {true, _} -> insert_terminal_node(btree, key, @deleted)
      {false, _} -> btree
    end
  end

  @spec commit(Btree.t()) :: Btree.t()

  def commit(tree = %Btree{store: store}) do
    Store.commit(store)
    tree
  end

  @spec key_range(Btree.t(), key, key, boolean) :: Btree.KeyRange.t()

  def key_range(tree, min_key \\ nil, max_key \\ nil, reverse \\ false) do
    Btree.KeyRange.new(tree, min_key, max_key, reverse)
  end

  def __leaf__, do: @leaf
  def __branch__, do: @branch
  def __value__, do: @value
  def __deleted__, do: @deleted

  @spec insert_terminal_node(Btree.t(), key, terminal_node) :: Btree.t()

  defp insert_terminal_node(btree, key, terminal_node) do
    %Btree{root: root, store: store, capacity: cap, size: s} = btree

    {leaf = {@leaf, children}, path} = lookup_leaf(root, store, key, [])
    {root_loc, new_root} = build_up(store, leaf, [{key, terminal_node}], [], path, cap)

    s =
      case terminal_node do
        {@value, _} -> if List.keymember?(children, key, 0), do: s, else: s + 1
        @deleted -> if List.keymember?(children, key, 0), do: s - 1, else: s
      end

    Store.put_header(store, {s, root_loc})
    %Btree{root: new_root, root_loc: root_loc, capacity: cap, store: store, size: s}
  end

  defp load_node(store, key, node, [], _, _) do
    loc = Store.put_node(store, node)
    [[{key, loc}]]
  end

  defp load_node(store, key, node, [children | rest], level, cap) do
    loc = Store.put_node(store, node)
    children = [{key, loc} | children]

    if length(children) == cap do
      parent = make_node(children, level)
      parent_key = List.last(keys(children))
      [[] | load_node(store, parent_key, parent, rest, level + 1, cap)]
    else
      [children | rest]
    end
  end

  defp finalize_load(store, [children], level, _) do
    case children do
      [{_, loc}] when level > 1 ->
        {Store.get_node(store, loc), loc}

      _ ->
        node = make_node(children, level)
        {node, Store.put_node(store, node)}
    end
  end

  defp finalize_load(store, [children | rest], level, cap) do
    case children do
      [] ->
        finalize_load(store, rest, level + 1, cap)

      _ ->
        node = make_node(children, level)
        key = List.last(keys(children))
        stack = load_node(store, key, node, rest, level + 1, cap)
        finalize_load(store, stack, level + 1, cap)
    end
  end

  defp make_node(children, level) do
    children = Enum.reverse(children)
    if level == 1, do: {@leaf, children}, else: {@branch, children}
  end

  defp lookup_leaf(branch = {@branch, children}, store, key, path) do
    loc =
      Enum.reduce_while(children, nil, fn
        {_, loc}, nil ->
          {:cont, loc}

        {k, loc}, acc ->
          if k <= key, do: {:cont, loc}, else: {:halt, acc}
      end)

    child = Store.get_node(store, loc)

    lookup_leaf(child, store, key, [branch | path])
  end

  defp lookup_leaf(leaf = {@leaf, _}, _, _, path) do
    {leaf, path}
  end

  defp build_up(store, node, to_merge, to_delete, [], cap) do
    to_merge_locs = store_nodes(store, to_merge)

    case replace_node(store, node, to_merge_locs, to_delete, nil, cap) do
      [] ->
        root = leaf()
        {Store.put_node(store, root), root}

      [{_, {@branch, [{_, loc}]}}] ->
        {loc, Store.get_node(store, loc)}

      [{_, node}] ->
        {Store.put_node(store, node), node}

      new_nodes ->
        new_locs = store_nodes(store, new_nodes)
        root = {@branch, new_locs}
        {Store.put_node(store, root), root}
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
    merged =
      Enum.reduce(merge, children, fn kv = {k, _}, acc ->
        List.keystore(acc, k, 0, kv)
      end)

    Enum.reduce(delete, merged, fn k, acc ->
      List.keydelete(acc, k, 0)
    end)
    |> List.keysort(0)
  end

  defp wrap_nodes(chunks, type) do
    for chunk = [{k, _} | _] <- chunks do
      {k, {type, chunk}}
    end
  end

  defp split_merge(children, store, old_node, parent, cap) do
    size = length(children)

    cond do
      size > cap ->
        split(children, cap)

      size < div(cap + 1, 2) and parent != nil and old_node != nil ->
        merge(store, children, old_node, parent, cap)

      true ->
        [children]
    end
  end

  defp split(children, cap) do
    children
    |> Enum.split(div(cap + 1, 2))
    |> Tuple.to_list()
  end

  defp merge(store, children, {_, old_children}, parent, cap) do
    key = min_key(keys(old_children), keys(children))

    (left_sibling(store, parent, key) ++ children)
    |> split_merge(store, nil, parent, cap)
  end

  defp left_sibling(store, {@branch, children}, key) do
    left =
      children
      |> Enum.take_while(fn {k, _} -> k < key end)
      |> List.last()

    case left do
      {_, loc} ->
        {_, children} = Store.get_node(store, loc)
        children

      nil ->
        []
    end
  end

  defp keys(tuples) do
    Enum.map(tuples, &elem(&1, 0))
  end

  defp min_key([], ks2), do: List.first(ks2)
  defp min_key(ks1, []), do: List.first(ks1)
  defp min_key(ks1, ks2), do: min(List.first(ks1), List.first(ks2))
end

defimpl Enumerable, for: CubDB.Btree do
  alias CubDB.Btree
  alias CubDB.Store

  @value Btree.__value__()
  @deleted Btree.__deleted__()

  def reduce(btree, cmd_acc, fun) do
    Btree.Enumerable.reduce(btree, cmd_acc, fun, &get_children/2)
  end

  def count(%Btree{size: size}), do: {:ok, size}

  def member?(btree, {key, value}) do
    case Btree.has_key?(btree, key) do
      {true, ^value} -> {:ok, true}
      _ -> {:ok, false}
    end
  end

  def member?(_, _), do: {:ok, false}

  def slice(_), do: {:error, __MODULE__}

  defp get_children({@value, v}, _), do: v

  defp get_children({_, locs}, store) do
    locs
    |> Enum.map(fn {k, loc} ->
      {k, Store.get_node(store, loc)}
    end)
    |> Enum.filter(fn {_, node} ->
      node != @deleted
    end)
  end
end
