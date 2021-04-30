defmodule CubDB.Store.TestStore do
  @moduledoc false

  # `CubDB.Store.TestStore` is an implementation of the `Store` protocol
  # intended for test purposes only. It is backed by a map, but supports all the
  # operations of a `CubDB.Store`. It allows some tests to be simpler and faster
  # by avoid using the file system.

  defstruct agent: nil
  alias CubDB.Store.TestStore

  @type t :: %TestStore{agent: pid}

  @spec create() :: {:ok, t} | {:error, term}

  def create do
    with {:ok, pid} <- Agent.start_link(fn -> {%{}, nil} end) do
      {:ok, %TestStore{agent: pid}}
    end
  end
end

defimpl CubDB.Store, for: CubDB.Store.TestStore do
  alias CubDB.Store.TestStore

  def put_node(%TestStore{agent: agent}, node) do
    Agent.get_and_update(
      agent,
      fn {map, latest_header_loc} ->
        loc = Enum.count(map)
        {loc, {Map.put(map, loc, node), latest_header_loc}}
      end,
      :infinity
    )
  end

  def put_header(%TestStore{agent: agent}, header) do
    Agent.get_and_update(
      agent,
      fn {map, _} ->
        loc = Enum.count(map)
        {loc, {Map.put(map, loc, header), loc}}
      end,
      :infinity
    )
  end

  def sync(%TestStore{}), do: :ok

  def get_node(%TestStore{agent: agent}, location) do
    case Agent.get(
           agent,
           fn {map, _} ->
             Map.fetch(map, location)
           end,
           :infinity
         ) do
      {:ok, value} -> value
      :error -> raise(ArgumentError, message: "End of file")
    end
  end

  def get_latest_header(%TestStore{agent: agent}) do
    Agent.get(
      agent,
      fn
        {_, nil} -> nil
        {map, header_loc} -> {header_loc, Map.get(map, header_loc)}
      end,
      :infinity
    )
  end

  def close(%TestStore{agent: agent}) do
    Agent.stop(agent, :normal, :infinity)
  end

  def blank?(%TestStore{agent: agent}) do
    Agent.get(
      agent,
      fn
        {_, nil} -> true
        _ -> false
      end,
      :infinity
    )
  end

  def open?(%TestStore{agent: agent}) do
    Process.alive?(agent)
  end
end
