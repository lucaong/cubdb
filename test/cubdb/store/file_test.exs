defmodule CubDB.Store.FileTest do
  use CubDB.StoreExamples, async: true

  setup do
    tmp_path = :lib.nonl(:os.cmd('mktemp'))
    on_exit(fn -> :file.delete(tmp_path) end)
    {:ok, store: CubDB.Store.File.new(tmp_path)}
  end
end

