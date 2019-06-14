ExUnit.configure(exclude: [property_based: true])
ExUnit.start()

defmodule TestHelper do
  def make_btree(store, entries, cap \\ 32) do
    Enum.reduce(entries, CubDB.Btree.new(store, cap), fn {key, value}, btree ->
      CubDB.Btree.insert(btree, key, value)
    end)
  end
end
