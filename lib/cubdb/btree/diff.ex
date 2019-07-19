defmodule CubDB.Btree.Diff do
  @moduledoc false

  # `CubDB.Btree.Diff` is a module implementing the `Enumerable` protocol to
  # iterate over each update operation that occurred between two Btree
  # representing different snapshots of the same store.  This is used after a
  # compaction operation to allow the compacted Btree to catch-up with updates
  # performed after the compaction started.

  @enforce_keys [:from_btree, :to_btree]
  defstruct from_btree: nil, to_btree: nil

  alias CubDB.Btree
  alias CubDB.Btree.Diff

  @type t :: %Diff{from_btree: Btree.t(), to_btree: Btree.t()}

  @spec new(Btree.t(), Btree.t()) :: t

  def new(from_btree, to_btree) do
    if from_btree.store != to_btree.store,
      do:
        raise(ArgumentError,
          message: "CubDB.Btree.Diff can only be created from Btree sharing the same store"
        )

    %Diff{from_btree: from_btree, to_btree: to_btree}
  end
end

defimpl Enumerable, for: CubDB.Btree.Diff do
  alias CubDB.Btree
  alias CubDB.Store
  alias CubDB.Btree.Diff

  @value Btree.__value__()
  @deleted Btree.__deleted__()

  def reduce(%Diff{from_btree: %Btree{root_loc: root_loc}, to_btree: to_btree}, cmd_acc, fun) do
    Btree.Enumerable.reduce(to_btree, cmd_acc, fun, &get_children(root_loc, &1, &2))
  end

  def count(_), do: {:error, __MODULE__}

  def member?(%Diff{}, _), do: {:error, __MODULE__}

  def slice(_), do: {:error, __MODULE__}

  defp get_children(_, value = {@value, _}, _), do: value

  defp get_children(_, @deleted, _), do: @deleted

  defp get_children(from_root_loc, {_, locs}, store) do
    locs
    |> Enum.filter(fn
      {_, loc} -> loc > from_root_loc
    end)
    |> Enum.map(fn {k, loc} ->
      {k, Store.get_node(store, loc)}
    end)
  end
end
