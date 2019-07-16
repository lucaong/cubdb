defmodule CubDB.Btree.KeyRange do
  @moduledoc false

  # `CubDB.Btree.KeyRange` is a module implementing the `Enumerable` protocol to
  # iterate through a range of entries on a Btree bounded by a minimum and
  # maximum key. The bounds can be exclusive or inclusive. This is primarily
  # used for selection operations.

  alias CubDB.Btree
  alias CubDB.Btree.KeyRange

  @type bound :: {Btree.key(), :included | :excluded} | nil
  @type t :: %KeyRange{btree: Btree.t(), min_key: bound, max_key: bound, reverse: boolean}

  @enforce_keys [:btree]
  defstruct btree: nil, min_key: nil, max_key: nil, reverse: false

  @spec new(Btree.t(), bound, bound, boolean) :: KeyRange.t()
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

  def reduce(key_range, cmd_acc, fun) do
    %KeyRange{btree: btree, min_key: min_key, max_key: max_key, reverse: reverse} = key_range
    Btree.Enumerable.reduce(btree, cmd_acc, fun, &get_children(min_key, max_key, reverse, &1, &2))
  end

  def count(_), do: {:error, __MODULE__}

  def member?(%KeyRange{min_key: {min, :included}, max_key: _}, {key, _}) when key < min do
    {:ok, false}
  end

  def member?(%KeyRange{min_key: _, max_key: {max, :included}}, {key, _}) when key > max do
    {:ok, false}
  end

  def member?(%KeyRange{min_key: {min, :excluded}, max_key: _}, {key, _}) when key <= min do
    {:ok, false}
  end

  def member?(%KeyRange{min_key: _, max_key: {max, :excluded}}, {key, _}) when key >= max do
    {:ok, false}
  end

  def member?(%KeyRange{btree: btree}, {key, value}) do
    case Btree.fetch(btree, key) do
      {:ok, ^value} -> {:ok, true}
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
        [{key, _}, {next_key, _}] -> filter_branch(min_key, max_key, key, next_key)
        [{key, _}] -> filter_branch(nil, max_key, key, nil)
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
        filter_leave(min_key, max_key, key)
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

  defp filter_branch(nil, nil, _, _), do: true
  defp filter_branch(nil, {max, :included}, key, _), do: key <= max
  defp filter_branch(nil, {max, :excluded}, key, _), do: key < max
  defp filter_branch({min, _}, nil, _, next_key), do: next_key > min
  defp filter_branch({min, _}, {max, :included}, key, next_key), do: key <= max && next_key > min
  defp filter_branch({min, _}, {max, :excluded}, key, next_key), do: key < max && next_key > min

  defp filter_leave(nil, nil, _), do: true
  defp filter_leave({min, :included}, nil, key), do: key >= min
  defp filter_leave({min, :excluded}, nil, key), do: key > min
  defp filter_leave(nil, {max, :included}, key), do: key <= max
  defp filter_leave(nil, {max, :excluded}, key), do: key < max
  defp filter_leave({min, :included}, {max, :included}, key), do: key >= min && key <= max
  defp filter_leave({min, :excluded}, {max, :included}, key), do: key > min && key <= max
  defp filter_leave({min, :included}, {max, :excluded}, key), do: key >= min && key < max
  defp filter_leave({min, :excluded}, {max, :excluded}, key), do: key > min && key < max
end
