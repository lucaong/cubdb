defmodule CubDB.Store.MemMap do
  @moduledoc """
  Dummy implementation of Store based on an in-memory map, only meant for
  testing purposes
  """

  defstruct agent: nil
  alias CubDB.Store.MemMap

  def new do
    with {:ok, pid} <- Agent.start_link(fn -> {%{}, nil} end) do
      %MemMap{agent: pid}
    end
  end
end

defimpl CubDB.Store, for: CubDB.Store.MemMap do
  alias CubDB.Store.MemMap

  def put_node(%MemMap{agent: agent}, node) do
    Agent.get_and_update(agent, fn {map, latest_header_loc} ->
      loc = Enum.count(map)
      if elem(node, 0) == :Btree do
        {loc, {Map.put(map, loc, node), loc}}
      else
        {loc, {Map.put(map, loc, node), latest_header_loc}}
      end
    end)
  end

  def get_node(%MemMap{agent: agent}, location) do
    Agent.get(agent, fn {map, _} ->
      Map.get(map, location, {:error, "No node found at location #{location}"})
    end)
  end

  def get_latest_header(%MemMap{agent: agent}) do
    Agent.get(agent, fn
      {_, nil} -> nil
      {map, header_loc} -> {header_loc, Map.get(map, header_loc)}
    end)
  end
end
