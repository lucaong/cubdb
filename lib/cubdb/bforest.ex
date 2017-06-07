defmodule CubDB.Bforest do
  alias CubDB.Btree
  alias CubDB.Bforest

  @type bforest :: %Bforest{btrees: nonempty_list(%Btree{})}
  @type key :: any
  @type val :: any

  @enforce_keys [:btrees]
  defstruct btrees: nil

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
end
