defmodule CubDB.Btree.Enumerable do
  alias CubDB.Btree
  alias CubDB.Store

  @leaf Btree.__leaf__
  @branch Btree.__branch__
  @value Btree.__value__

  @spec reduce(%Btree{}, Enumerable.acc, Enumerable.reducer, (Btree.btree_node, Store.t -> any)) :: Enumerable.result
  def reduce(%Btree{root: root, store: store}, cmd_acc, fun, get_children) do
    do_reduce({[], [[{nil, root}]]}, cmd_acc, fun, get_children, store)
  end

  defp do_reduce(_, {:halt, acc}, _, _, _), do: {:halted, acc}

  defp do_reduce(t, {:suspend, acc}, fun, get_children, store) do
    {:suspended, acc, &do_reduce(t, &1, fun, get_children, store)}
  end

  defp do_reduce(t, {:cont, acc}, fun, get_children, store) do
    case next(t, store, get_children) do
      {t, item} -> do_reduce(t, fun.(item, acc), fun, get_children, store)
      :done -> {:done, acc}
    end
  end

  defp next({[], [[] | todo]}, store, get_children) do
    case todo do
      [] -> :done
      _ -> next({[], todo}, store, get_children)
    end
  end

  defp next({[], [[{_, leaf = {@leaf, _}} | rest] | todo]}, store, get_children) do
    children = get_children.(leaf, store)

    next({children, [rest | todo]}, store, get_children)
  end

  defp next({[], [[{_, branch = {@branch, _}} | rest] | todo]}, store, get_children) do
    children = get_children.(branch, store)

    next({[], [children | [rest | todo]]}, store, get_children)
  end

  defp next({[{k, value = {@value, _}} | rest], todo}, store, get_children) do
    {{rest, todo}, {k, get_children.(value, store)}}
  end
end
