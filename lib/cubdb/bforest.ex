defmodule CubDB.Bforest do
  alias CubDB.Btree
  alias CubDB.Bforest

  @type bforest :: %Bforest{btrees: nonempty_list(%Btree{})}
  @type key :: any
  @type val :: any

  @enforce_keys [:btrees]
  defstruct btrees: nil

  @moduledoc """
  A Bforest contains a list of Btrees. All write operations are made on
  the first Btree (called live tree), while lookup operations are tried
  on all trees in order, and the first result is returned.
  """

  @spec new(nonempty_list(%Btree{})) :: bforest
  def new(btrees) do
    %Bforest{btrees: btrees}
  end

  @spec lookup(bforest, key) :: val | nil
  def lookup(forest = %Bforest{}, key) do
    case has_key?(forest, key) do
      {false, _} -> nil
      {true, value} -> value
    end
  end

  @spec has_key?(bforest, key) :: {true, val} | {false, nil}
  def has_key?(%Bforest{btrees: btrees}, key) do
    Enum.reduce_while(btrees, nil, fn tree, _ ->
      case Btree.has_key?(tree, key) do
        tuple = {true, _} -> {:halt, tuple}
        tuple -> {:cont, tuple}
      end
    end)
  end

  @spec insert(bforest, key, val) :: bforest
  def insert(%Bforest{btrees: [live_tree | rest]}, key, value) do
    %Bforest{btrees: [Btree.insert(live_tree, key, value) | rest]}
  end

  @spec delete(bforest, key) :: bforest
  def delete(%Bforest{btrees: [live_tree | rest]}, key) do
    %Bforest{btrees: [Btree.delete(live_tree, key) | rest]}
  end

  @spec commit(bforest) :: bforest
  def commit(%Bforest{btrees: [live_tree | rest]}) do
    %Bforest{btrees: [Btree.commit(live_tree) | rest]}
  end

  @spec compact(bforest, Store.t()) :: %Btree{}
  def compact(forest = %Bforest{}, store) do
    Btree.load(forest, store)
  end
end

defimpl Enumerable, for: CubDB.Bforest do
  alias CubDB.Bforest

  def reduce(%Bforest{btrees: trees}, cmd_acc, fun) do
    tuples =
      Enum.map(trees, fn tree ->
        Enumerable.reduce(tree, cmd_acc, &step/2)
      end)

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
