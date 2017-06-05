defmodule CubDB.Store.FileTest do
  use CubDB.StoreExamples, async: true

  setup do
    tmp_path = :lib.nonl(:os.cmd('mktemp'))
    store = CubDB.Store.File.new(tmp_path)

    on_exit(fn ->
      :file.delete(tmp_path)
    end)

    {:ok, store: store}
  end
end

