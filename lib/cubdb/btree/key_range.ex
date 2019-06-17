defmodule CubDB.Btree.KeyRange do
  alias CubDB.Btree
  alias CubDB.Btree.KeyRange

  @type t :: %KeyRange{btree: Btree.t(), from: Btree.key() | nil, to: Btree.key() | nil, reverse: boolean}

  @enforce_keys [:btree]
  defstruct btree: nil, from: nil, to: nil, reverse: false

  @spec new(Btree.t(), Btree.key(), Btree.key(), boolean) :: KeyRange.t()
  def new(btree, from \\ nil, to \\ nil, reverse \\ false) do
    %KeyRange{btree: btree, from: from, to: to, reverse: reverse}
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

  def reduce(%KeyRange{btree: btree, from: from, to: to, reverse: reverse}, cmd_acc, fun) do
    Btree.Enumerable.reduce(btree, cmd_acc, fun, &get_children(from, to, reverse, &1, &2))
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

  defp get_children(from, to, reverse, {@branch, locs}, store) do
    children =
      locs
      |> Enum.chunk_every(2, 1)
      |> Enum.filter(fn
        [{key, _}, {next_key, _}] -> (to == nil or key <= to) and (from == nil or next_key > from)
        [{key, _}] -> to == nil or key <= to
      end)
      |> Enum.map(fn [{k, loc} | _] ->
        {k, Store.get_node(store, loc)}
      end)
    if reverse, do: Enum.reverse(children), else: children
  end

  defp get_children(from, to, reverse, {@leaf, locs}, store) do
    children =
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

    if reverse, do: Enum.reverse(children), else: children
  end

  defp get_children(_, _, _, {@value, v}, _), do: v
end
