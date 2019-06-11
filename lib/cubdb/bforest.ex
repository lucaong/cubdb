defmodule CubDB.Bforest do
  alias CubDB.Store
  alias CubDB.Btree
  alias CubDB.Bforest

  @type bforest :: %Bforest{btree: %Btree{}, bforest: %Bforest{} | nil, frozen: boolean}
  @type key :: any
  @type val :: any

  @enforce_keys [:btree, :frozen]
  defstruct btree: nil, bforest: nil, frozen: false

  @moduledoc """
  A Bforest is a recursive structure that contains a Btree and, optionally, a
  Bforest. All write operations are made on the Btree (called the live tree),
  while lookup operations are tried on the Btree first, and then on the Bforest,
  and the first result is returned.
  """

  @spec new(%Btree{}, %Bforest{} | nil, boolean) :: bforest
  def new(btree, bforest \\ nil, frozen \\ false) do
    %Bforest{btree: btree, bforest: bforest, frozen: frozen}
  end

  @spec lookup(bforest, key) :: val | nil
  def lookup(forest = %Bforest{}, key) do
    case has_key?(forest, key) do
      {false, _} -> nil
      {true, value} -> value
    end
  end

  @spec has_key?(bforest, key) :: {true, val} | {false, nil}
  def has_key?(%Bforest{btree: btree, bforest: nil}, key) do
    Btree.has_key?(btree, key)
  end

  def has_key?(%Bforest{btree: btree, bforest: bforest}, key) do
    case Btree.has_key?(btree, key) do
      {false, _} -> has_key?(bforest, key)
      found -> found
    end
  end

  @spec insert(bforest, key, val) :: bforest
  def insert(bforest, key, value) do
    ensure_editable!(bforest)
    %Bforest{bforest | btree: Btree.insert(bforest.btree, key, value)}
  end

  @spec delete(bforest, key) :: bforest
  def delete(bforest, key) do
    ensure_editable!(bforest)
    %Bforest{bforest | btree: Btree.delete(bforest.btree, key)}
  end

  @spec commit(bforest) :: bforest
  def commit(bforest) do
    ensure_editable!(bforest)
    %Bforest{bforest | btree: Btree.commit(bforest.btree)}
  end

  @spec compact(bforest, Store.t()) :: %Btree{}
  def compact(forest = %Bforest{}, store) do
    Btree.load(forest, store)
  end

  defp ensure_editable!(bforest) do
    if bforest.frozen,
      do: raise(ArgumentError, message: "cannot modify frozen Bforest")
  end
end

defimpl Enumerable, for: CubDB.Bforest do
  alias CubDB.Bforest

  def reduce(%Bforest{btree: btree, bforest: nil}, cmd_acc, fun) do
    Enumerable.reduce(btree, cmd_acc, fun)
  end

  def reduce(bforest, cmd_acc, fun) do
    tuples = get_tuples(bforest, cmd_acc, [])
    do_reduce(tuples, cmd_acc, fun)
  end

  # TODO: implement efficiently
  def count(%Bforest{}), do: {:error, __MODULE__}

  def member?(forest = %Bforest{}, {key, value}) do
    case Bforest.has_key?(forest, key) do
      {true, ^value} -> {:ok, true}
      _ -> {:ok, false}
    end
  end

  def slice(_), do: {:error, __MODULE__}

  defp get_tuples(%Bforest{btree: btree, bforest: nil}, cmd_acc, tuples) do
    Enum.reverse([Enumerable.reduce(btree, cmd_acc, &step/2) | tuples])
  end

  defp get_tuples(%Bforest{btree: btree, bforest: bforest}, cmd_acc, tuples) do
    get_tuples(bforest, cmd_acc, [Enumerable.reduce(btree, cmd_acc, &step/2) | tuples])
  end

  defp step(x, _) do
    {:suspend, x}
  end

  defp do_reduce(tuples, {:halt, acc}, _) do
    Enum.each(tuples, fn {_, _, fun} -> fun.({:halt, nil}) end)
    {:halted, acc}
  end

  defp do_reduce(tuples, {:suspend, acc}, fun) do
    {:suspended, acc, &do_reduce(tuples, &1, fun)}
  end

  defp do_reduce(tuples, {:cont, acc}, fun) do
    tuples =
      Enum.filter(tuples, fn tuple ->
        elem(tuple, 0) != :done
      end)

    min = Enum.min_by(tuples, fn {_, {k, _}, _} -> k end, fn -> nil end)

    case min do
      nil ->
        {:done, acc}

      {:suspended, item = {key, _}, _} ->
        tuples =
          Enum.map(tuples, fn fun_val = {_, {k, _}, f} ->
            if k == key do
              f.({:cont, nil})
            else
              fun_val
            end
          end)

        do_reduce(tuples, fun.(item, acc), fun)
    end
  end
end
