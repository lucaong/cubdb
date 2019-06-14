defmodule CubDB.Btree.KeyRange do
  @enforce_keys [:btree]
  defstruct btree: nil, from: nil, to: nil

  alias CubDB.Btree
  alias CubDB.Btree.KeyRange

  @spec new(%Btree{}, Btree.key, Btree.key) :: %KeyRange{}
  def new(btree, from \\ nil, to \\ nil) do
    %KeyRange{btree: btree, from: from, to: to}
  end
end

defimpl Enumerable, for: CubDB.Btree.KeyRange do
  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Btree.KeyRange

  @leaf Btree.__leaf__
  @branch Btree.__branch__
  @value Btree.__value__
  @deleted Btree.__deleted__

  def reduce(%KeyRange{btree: btree, from: from, to: to}, cmd_acc, fun) do
    Btree.Enumerable.reduce(btree, cmd_acc, fun, &get_children(from, to, &1, &2))
  end

  def count(_), do: {:error, __MODULE__}

  def member?(%KeyRange{from: from, to: to}, {key, _})
  when (is_nil(from) == false and key < from) or (is_nil(to) == false and key > to) do
    {:ok, false}
  end

  def member?(%KeyRange{btree: btree}, {key, value}) do
    case Btree.has_key?(btree, key) do
      {true, ^value} -> {:ok, true}
      _ -> {:ok, false}
    end
  end

  def member?(_, _), do: {:ok, false}

  def slice(_), do: {:error, __MODULE__}

  defp get_children(from, to, {@branch, locs}, store) do
    locs
    |> Enum.chunk_every(2, 1)
    |> Enum.filter(fn
      [{key, _}, {next_key, _}] -> (to == nil or key <= to) and (from == nil or next_key > from)
      [{key, _}] -> (to == nil or key <= to)
    end)
    |> Enum.map(fn [{k, loc} | _] ->
      {k, Store.get_node(store, loc)}
    end)
  end

  defp get_children(from, to, {@leaf, locs}, store) do
    locs
    |> Enum.filter(fn {key, _} ->
      (from == nil or key >= from) and (to == nil or key <= to)
    end)
    |> Enum.map(fn {k, loc} ->
      {k, Store.get_node(store, loc)}
    end)
    |> Enum.filter(fn {_, node} ->
      node != @deleted
    end)
  end

  defp get_children(_, _, {@value, v}, _), do: v
end
