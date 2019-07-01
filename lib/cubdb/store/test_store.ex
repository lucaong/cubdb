defmodule CubDB.Store.TestStore do
  @moduledoc false

  defstruct agent: nil
  alias CubDB.Store.TestStore

  def new do
    with {:ok, pid} <- Agent.start_link(fn -> {%{}, nil} end) do
      %TestStore{agent: pid}
    end
  end
end

defimpl CubDB.Store, for: CubDB.Store.TestStore do
  alias CubDB.Store.TestStore

  def put_node(%TestStore{agent: agent}, node) do
    Agent.get_and_update(agent, fn {map, latest_header_loc} ->
      loc = Enum.count(map)
      {loc, {Map.put(map, loc, node), latest_header_loc}}
    end)
  end

  def put_header(%TestStore{agent: agent}, header) do
    Agent.get_and_update(agent, fn {map, _} ->
      loc = Enum.count(map)
      {loc, {Map.put(map, loc, header), loc}}
    end)
  end

  def sync(%TestStore{}), do: :ok

  def get_node(%TestStore{agent: agent}, location) do
    Agent.get(agent, fn {map, _} ->
      Map.get(map, location, {:error, "No node found at location #{location}"})
    end)
  end

  def get_latest_header(%TestStore{agent: agent}) do
    Agent.get(agent, fn
      {_, nil} -> nil
      {map, header_loc} -> {header_loc, Map.get(map, header_loc)}
    end)
  end

  def close(%TestStore{}), do: :ok

  def blank?(%TestStore{agent: agent}) do
    Agent.get(agent, fn
      {_, nil} -> true
      _ -> false
    end)
  end
end
