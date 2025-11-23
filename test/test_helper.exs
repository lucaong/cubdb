assert_receive_timeout =
  case System.get_env("ASSERT_RECEIVE_TIMEOUT") do
    nil -> 200
    str -> String.to_integer(str)
  end

ExUnit.configure(
  exclude: [property_based: true],
  assert_receive_timeout: assert_receive_timeout
)

ExUnit.start()

defmodule TestHelper do
  def make_btree(store, entries, cap \\ 32) do
    Enum.reduce(entries, CubDB.Btree.new(store, cap), fn {key, value}, btree ->
      CubDB.Btree.insert(btree, key, value)
    end)
    |> CubDB.Btree.commit()
  end

  defmacro trapping_exit(do: block) do
    quote do
      original = Process.flag(:trap_exit, true)

      try do
        unquote(block)
      after
        Process.flag(:trap_exit, original)
      end
    end
  end

  defmodule Btree.Utils do
    @moduledoc false

    alias CubDB.Btree
    alias CubDB.Store

    import Btree, only: [header: 1, value: 1, deleted: 0]

    def debug(store) do
      {_, {size, root_loc, _, _}} = Store.get_latest_header(store)
      {:Btree, size, debug_node(store, root_loc)}
    end

    defp debug_node(store, loc) do
      case Store.get_node(store, loc) do
        value(val: value) ->
          value

        deleted() ->
          deleted()

        {type, locs} ->
          children =
            Enum.map(locs, fn {k, loc} ->
              {k, debug_node(store, loc)}
            end)

          {type, children}
      end
    end

    def load(store, {:Btree, size, root}) do
      {root_loc, root_node} = load_node(store, root)
      Store.put_header(store, header(size: size, location: root_loc, dirt: 0, metadata: []))
      {root_loc, root_node}
    end

    defp load_node(store, {type, children}) do
      locs =
        Enum.map(children, fn {k, child} ->
          {loc, _} = load_node(store, child)
          {k, loc}
        end)

      node = {type, locs}
      {Store.put_node(store, node), node}
    end

    defp load_node(store, deleted()) do
      {Store.put_node(store, deleted()), deleted()}
    end

    defp load_node(store, value) do
      node = value(val: value)
      {Store.put_node(store, node), node}
    end
  end
end
