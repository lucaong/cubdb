defmodule CubDB.Btree.KeyRange do
  @moduledoc false

  alias CubDB.Btree
  alias CubDB.Btree.KeyRange

  @type t :: %KeyRange{btree: Btree.t(), min_key: Btree.key() | nil, max_key: Btree.key() | nil, reverse: boolean}

  @enforce_keys [:btree]
  defstruct btree: nil, min_key: nil, max_key: nil, reverse: false

  @spec new(Btree.t(), Btree.key(), Btree.key(), boolean) :: KeyRange.t()
  def new(btree, min_key \\ nil, max_key \\ nil, reverse \\ false) do
    %KeyRange{btree: btree, min_key: min_key, max_key: max_key, reverse: reverse}
  end
end

defimpl Enumerable, for: CubDB.Btree.KeyRange do
  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Btree.KeyRange

  @leaf Btree.__leaf__()
  @branch Btree.__branch__()
  @value Btree.__value__()
  @deleted Btree.__deleted__()

  def reduce(%KeyRange{btree: btree, min_key: min_key, max_key: max_key, reverse: reverse}, cmd_acc, fun) do
    Btree.Enumerable.reduce(btree, cmd_acc, fun, &get_children(min_key, max_key, reverse, &1, &2))
  end

  def count(_), do: {:error, __MODULE__}

  def member?(%KeyRange{min_key: min_key, max_key: max_key}, {key, _})
      when (is_nil(min_key) == false and key < min_key) or (is_nil(max_key) == false and key > max_key) do
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

  defp get_children(min_key, max_key, reverse, {@branch, locs}, store) do
    children =
      locs
      |> Enum.chunk_every(2, 1)
      |> Enum.filter(fn
        [{key, _}, {next_key, _}] -> (max_key == nil or key <= max_key) and (min_key == nil or next_key > min_key)
        [{key, _}] -> max_key == nil or key <= max_key
      end)
      |> Enum.map(fn [{k, loc} | _] ->
        {k, Store.get_node(store, loc)}
      end)
    if reverse, do: Enum.reverse(children), else: children
  end

  defp get_children(min_key, max_key, reverse, {@leaf, locs}, store) do
    children =
      locs
      |> Enum.filter(fn {key, _} ->
        (min_key == nil or key >= min_key) and (max_key == nil or key <= max_key)
      end)
      |> Enum.map(fn {k, loc} ->
        {k, Store.get_node(store, loc)}
      end)
      |> Enum.filter(fn {_, node} ->
        node != @deleted
      end)

    if reverse, do: Enum.reverse(children), else: children
  end

  defp get_children(_, _, _, {@value, v}, _), do: v
end
