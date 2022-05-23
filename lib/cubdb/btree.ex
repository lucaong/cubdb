defmodule CubDB.Btree do
  @moduledoc false

  # `CubDB.Btree` is the internal module implementing the fundamental data
  # structure for CubDB: an append-only, copy-on-write B+tree.
  #
  # Operations on Btree generally return another modified Btree, similarly to
  # immutable data structures like maps. The new Btree points to the updated
  # root, while the "old" Btree still points to the old one, effectively
  # maintaining an immutable snapshot.
  #
  # Updates are not committed, and will not be visible after a restart, until
  # `commit/1` is explicitly called. Also, they may not be durable until
  # `sync/1` is called (although the OS will eventually sync changes to disk
  # even if `sync/1` is not explicitly called).

  @leaf :l
  @branch :b
  @value :v
  @deleted :d

  require Record
  Record.defrecord(:leaf, @leaf, children: [])
  Record.defrecord(:branch, @branch, children: [])
  Record.defrecord(:value, @value, val: nil)

  @type key :: CubDB.key()
  @type val :: CubDB.value()
  @type btree_size :: non_neg_integer
  @type dirt :: non_neg_integer
  @type location :: non_neg_integer
  @type capacity :: pos_integer
  @type child_pointer :: {key, location}
  @type leaf_node :: record(:leaf, children: [child_pointer])
  @type branch_node :: record(:branch, children: [child_pointer])
  @type value_node :: record(:value, val: val)
  @type deleted_node :: :d
  @type internal_node :: leaf_node | branch_node
  @type terminal_node :: value_node | deleted_node
  @type btree_node :: leaf_node | branch_node | value_node | deleted_node
  @type btree_header :: {btree_size, location, dirt}
  @type node_type :: :l | :b | :v | :d

  alias CubDB.Btree
  alias CubDB.Store

  @type t :: %Btree{
          root: branch_node | leaf_node,
          root_loc: location,
          size: btree_size,
          store: Store.t(),
          capacity: non_neg_integer
        }

  @default_capacity 32
  @enforce_keys [:root, :root_loc, :size, :store, :capacity]
  defstruct root: nil, root_loc: nil, size: 0, dirt: 0, store: nil, capacity: @default_capacity

  @spec new(Store.t(), pos_integer) :: Btree.t()

  def new(store, cap \\ @default_capacity) do
    case Store.get_latest_header(store) do
      {_, {s, loc, dirt}} ->
        root = Store.get_node(store, loc)
        %Btree{root: root, root_loc: loc, dirt: dirt, size: s, capacity: cap, store: store}

      nil ->
        root = leaf()
        loc = Store.put_node(store, root)
        Store.put_header(store, {0, loc, 0})
        %Btree{root: root, root_loc: loc, size: 0, capacity: cap, store: store}
    end
  end

  @spec load(Enumerable.t(), Store.t(), pos_integer) :: Btree.t()

  # `load/3` takes an enumerable of `{key, value}` entries that should be
  # strictly sorted by key and an empty store, and creates a Btree with those
  # entries by writing them into the store. It is used primarily for compaction
  # operations, where the enumerable is the `Btree` to be compacted. The fact
  # that the enumerable is sorted by key, allows using a specific algorithm to
  # create the Btree in a much faster way than with normal inserts.
  def load(enum, store, cap \\ @default_capacity) do
    unless Store.blank?(store),
      do: raise(ArgumentError, message: "cannot load into non-empty store")

    {st, count} =
      Enum.reduce(enum, {[], 0}, fn {k, v}, {st, count} ->
        {load_node(store, k, value(val: v), st, 1, cap), count + 1}
      end)

    if count == 0 do
      new(store, cap)
    else
      {root, root_loc} = finalize_load(store, st, 1, cap)
      Store.put_header(store, {count, root_loc, 0})
      %Btree{root: root, root_loc: root_loc, capacity: cap, store: store, size: count}
    end
  end

  @spec fetch(Btree.t(), key) :: {:ok, val} | :error

  # `fetch/2` returns `{:ok, value}` if an entry with key `key` is present
  # in the Btree, or `:error` otherwise.
  def fetch(%Btree{root: root, store: store}, key) do
    {{@leaf, children}, _} = lookup_leaf(root, store, key, [])

    case Enum.find(children, &match?({^key, _}, &1)) do
      nil ->
        :error

      {_, loc} ->
        case Store.get_node(store, loc) do
          {@value, value} -> {:ok, value}
          @deleted -> :error
        end
    end
  end

  @spec written_since?(Btree.t(), key, Btree.t()) ::
          boolean | {:maybe, :not_found} | {:maybe, :different_store}

  # `written_since?/3` returns `true` if the entry with the given key was
  # written in the btree since the reference btree (which should be an older
  # version of the same btree), or `false` if it was not written.
  #
  # There are two cases when the function cannot determine for sure if the key
  # was written: if the btree changed but the key is not present, in which case
  # it returns `{:maybe, :not_found}`, or if the store of the two btrees is
  # different, in which case it returns `{:maybe, :different_store}`.
  def written_since?(%Btree{store: store}, _key, %Btree{store: reference_store})
      when reference_store != store,
      do: {:maybe, :different_store}

  def written_since?(%Btree{root_loc: loc}, _key, %Btree{root_loc: loc}), do: false

  def written_since?(%Btree{root: root, store: store}, key, reference_btree) do
    %Btree{root_loc: reference_loc} = reference_btree

    case key_past_location?(root, store, key, reference_loc) do
      :not_found ->
        {:maybe, :not_found}

      boolean_value ->
        boolean_value
    end
  end

  @spec insert(Btree.t(), key, val) :: Btree.t()

  # `insert/3` writes an entry in the Btree, updating the previous one with the
  # same key if existing. It does not commit the operation, so `commit/1` must
  # be explicitly called to commit the insertion.
  def insert(btree, key, value) do
    insert_terminal_node(btree, key, value(val: value))
  end

  @spec insert_new(Btree.t(), key, val) :: Btree.t() | {:error, :exists}

  # `insert_new/3` writes an entry in the Btree, only if it is not yet present
  # in the database. It does not commit the operation, so `commit/1` must be
  # explicitly called to commit the insertion.
  def insert_new(btree, key, value) do
    insert_terminal_node(btree, key, value(val: value), false)
  end

  @spec delete(Btree.t(), key) :: Btree.t()

  # `delete/2` deletes the entry associated to `key` in the Btree, if existing.
  # It does not commit the operation, so `commit/1` must be explicitly called to
  # commit the deletion.
  def delete(btree, key) do
    %Btree{root: root, store: store, capacity: cap, size: s, dirt: dirt} = btree
    {leaf = {@leaf, children}, path} = lookup_leaf(root, store, key, [])

    case List.keyfind(children, key, 0) do
      {^key, loc} ->
        size =
          case Store.get_node(store, loc) do
            @deleted -> s
            _ -> s - 1
          end

        {root_loc, new_root} = build_up(store, leaf, [], [key], path, cap)

        %Btree{
          root: new_root,
          root_loc: root_loc,
          capacity: cap,
          store: store,
          size: size,
          dirt: dirt + 1
        }

      nil ->
        btree
    end
  end

  @spec mark_deleted(Btree.t(), key) :: Btree.t()

  # `mark_deleted/2` deletes an entry by marking it as deleted, as opposed to
  # `delete/2`, that simply removes it. It is necessary to use `mark_deleted/2`
  # instead of `delete/2` while a compaction operation is running in the
  # background. This is so that, after the compaction is done, the compacted
  # Btree can catch-up with updates performed after compaction started,
  # including deletions, that would otherwise not be enumerated by Btree.Diff.
  # Similar to `update/3` and `delete/3`, it does not commit the operation, so
  # `commit/1` must be explicitly called to commit the deletion.
  def mark_deleted(btree, key) do
    case fetch(btree, key) do
      {:ok, _} -> insert_terminal_node(btree, key, @deleted)
      :error -> btree
    end
  end

  @spec clear(Btree.t()) :: Btree.t()

  def clear(btree) do
    %Btree{store: store, capacity: cap, dirt: dirt} = btree

    root = leaf()
    loc = Store.put_node(store, root)
    %Btree{root: root, root_loc: loc, size: 0, dirt: dirt + 1, capacity: cap, store: store}
  end

  @spec commit(Btree.t()) :: Btree.t()

  # `commit/1` writes the header to the store, committing all updates performed
  # after the previous call to `commit/1`. This is primarily used to control
  # atomicity of updates: if a batch of updates is to be performed atomically,
  # `commit/1` must be called once, after all updates.
  # If one or more updates are performed, but `commit/1` is not called, the
  # updates won't be committed to the database and will be lost in case of a
  # restart.
  def commit(tree = %Btree{store: store, size: size, root_loc: root_loc, dirt: dirt}) do
    Store.put_header(store, {size, root_loc, dirt + 1})
    tree
  end

  @spec key_range(Btree.t(), Btree.KeyRange.bound(), Btree.KeyRange.bound(), boolean) ::
          Btree.KeyRange.t()

  # `key_range/4` returns a `Btree.KeyRange` `Enumerable` that can be used to
  # iterate through a range of entries with key between `min_key` and `max_key`.
  def key_range(tree, min_key \\ nil, max_key \\ nil, reverse \\ false) do
    Btree.KeyRange.new(tree, min_key, max_key, reverse)
  end

  @spec dirt_factor(Btree.t()) :: float

  # `dirt_factor/1` returns a flating point number between 0 and 1 giving an
  # indication of how much overhead due to old entries (that were rewritten or
  # deleted and are therefore unreachable) and headers is present in the Btree.
  # The dirt factor is used to estimate when a compaction operation is
  # necessary.
  def dirt_factor(%Btree{size: size, dirt: dirt}) do
    dirt / (1 + size + dirt)
  end

  @spec sync(Btree.t()) :: Btree.t()

  # `sync/1` performs a file sync on the store, and is used to ensure durability
  # of updates.
  def sync(btree = %Btree{store: store}) do
    :ok = Store.sync(store)
    btree
  end

  @spec stop(Btree.t()) :: :ok

  def stop(%Btree{store: store}) do
    Store.close(store)
  end

  @spec alive?(Btree.t()) :: boolean

  def alive?(%Btree{store: store}) do
    Store.open?(store)
  end

  def __leaf__, do: @leaf
  def __branch__, do: @branch
  def __value__, do: @value
  def __deleted__, do: @deleted

  @spec insert_terminal_node(Btree.t(), key, terminal_node, boolean) ::
          Btree.t() | {:error, :exists}

  defp insert_terminal_node(btree, key, terminal_node, overwrite \\ true) do
    %Btree{root: root, store: store, capacity: cap, size: s, dirt: dirt} = btree

    {leaf = {@leaf, children}, path} = lookup_leaf(root, store, key, [])
    was_set = child_is_set?(store, children, key)

    if overwrite == false && was_set do
      {:error, :exists}
    else
      {root_loc, new_root} = build_up(store, leaf, [{key, terminal_node}], [], path, cap)

      size =
        case {terminal_node, was_set} do
          {{@value, _}, true} -> s
          {{@value, _}, false} -> s + 1
          {@deleted, true} -> s - 1
          {@deleted, false} -> s
        end

      %Btree{
        root: new_root,
        root_loc: root_loc,
        capacity: cap,
        store: store,
        size: size,
        dirt: dirt + 1
      }
    end
  end

  @spec child_is_set?(Store.t(), [child_pointer], key) :: boolean

  defp child_is_set?(store, children, key) do
    case List.keyfind(children, key, 0) do
      nil -> false
      {_, pos} -> Store.get_node(store, pos) != @deleted
    end
  end

  @spec load_node(Store.t(), key, btree_node, [btree_node], pos_integer, capacity) :: [
          [child_pointer]
        ]

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

  @spec finalize_load(Store.t(), [[child_pointer]], pos_integer, capacity) ::
          {btree_node, location}

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

  @spec make_node([child_pointer], pos_integer) :: internal_node

  defp make_node(children, level) do
    children = Enum.reverse(children)
    if level == 1, do: leaf(children: children), else: branch(children: children)
  end

  @spec lookup_leaf(internal_node, Store.t(), key, [internal_node]) ::
          {leaf_node, [internal_node]}

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

  @spec key_past_location?(internal_node, Store.t(), key, location) :: boolean | :not_found

  defp key_past_location?({@branch, children}, store, key, target_loc) do
    loc =
      Enum.reduce_while(children, nil, fn
        {_, loc}, nil ->
          {:cont, loc}

        {k, loc}, acc ->
          if k <= key, do: {:cont, loc}, else: {:halt, acc}
      end)

    if loc > target_loc do
      key_past_location?(Store.get_node(store, loc), store, key, target_loc)
    else
      false
    end
  end

  defp key_past_location?({@leaf, children}, _store, key, target_loc) do
    case Enum.find(children, &match?({^key, _}, &1)) do
      nil ->
        :not_found

      {_, loc} ->
        loc > target_loc
    end
  end

  @spec build_up(Store.t(), internal_node, [{key, val}], [key], [internal_node], capacity) ::
          {location, internal_node}

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
        root = branch(children: new_locs)
        {Store.put_node(store, root), root}
    end
  end

  defp build_up(store, node, to_merge, to_delete, [parent | up], cap) do
    to_merge_locs = store_nodes(store, to_merge)
    new_nodes = replace_node(store, node, to_merge_locs, to_delete, parent, cap)
    deleted = keys(elem(node, 1)) -- keys(new_nodes)
    build_up(store, parent, new_nodes, deleted, up, cap)
  end

  @spec store_nodes(Store.t(), [{key, btree_node}]) :: [{key, location}]

  defp store_nodes(store, nodes) do
    Enum.map(nodes, fn {k, v} ->
      {k, Store.put_node(store, v)}
    end)
  end

  @spec replace_node(
          Store.t(),
          internal_node,
          [{key, location}],
          [key],
          internal_node | nil,
          capacity
        ) :: [btree_node]

  defp replace_node(store, node, merge, delete, parent, cap) do
    {type, children} = node

    children
    |> update_children(merge, delete)
    |> split_merge(store, node, parent, cap)
    |> wrap_nodes(type)
  end

  @spec update_children([child_pointer], [{key, location}], [key]) :: [child_pointer]

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

  @spec wrap_nodes([[{key, any}]], node_type) :: [{key, btree_node}]

  defp wrap_nodes(chunks, type) do
    for chunk = [{k, _} | _] <- chunks do
      {k, {type, chunk}}
    end
  end

  @spec split_merge(
          [child_pointer],
          Store.t(),
          internal_node | nil,
          internal_node | nil,
          capacity
        ) :: [[child_pointer]]

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

  @spec split([child_pointer], capacity) :: [[child_pointer]]

  defp split(children, cap) do
    children
    |> Enum.split(div(cap + 1, 2))
    |> Tuple.to_list()
  end

  @spec merge(Store.t(), [child_pointer], internal_node, internal_node, capacity) :: [
          [child_pointer]
        ]

  defp merge(store, children, {_, old_children}, parent, cap) do
    key = min_key(keys(old_children), keys(children))

    (left_sibling(store, parent, key) ++ children)
    |> split_merge(store, nil, parent, cap)
  end

  @spec left_sibling(Store.t(), branch_node, key) :: [child_pointer]

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

  @spec keys([{key, any}]) :: [key]

  defp keys(tuples) do
    Enum.map(tuples, &elem(&1, 0))
  end

  @spec min_key([key], [key]) :: key

  defp min_key([], ks2), do: List.first(ks2)
  defp min_key(ks1, []), do: List.first(ks1)
  defp min_key(ks1, ks2), do: min(List.first(ks1), List.first(ks2))
end

defimpl Enumerable, for: CubDB.Btree do
  # `Btree` implements `Enumerable`, and can be iterated (or streamed) yielding
  # entries sorted by key.

  alias CubDB.Btree
  alias CubDB.Store

  @value Btree.__value__()
  @deleted Btree.__deleted__()

  def reduce(btree, cmd_acc, fun) do
    Btree.Enumerable.reduce(btree, cmd_acc, fun, &get_children/2)
  end

  def count(%Btree{size: size}), do: {:ok, size}

  def member?(btree, {key, value}) do
    case Btree.fetch(btree, key) do
      {:ok, ^value} -> {:ok, true}
      _ -> {:ok, false}
    end
  end

  def member?(_, _), do: {:ok, false}

  def slice(_), do: {:error, __MODULE__}

  @spec get_children(Btree.btree_node(), Store.t()) :: any

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
