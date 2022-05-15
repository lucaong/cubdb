defmodule CubDB.Writer do
  @moduledoc false

  # The `CubDB.Writer` in an internal module that includes functions to obtain
  # write access to the database 

  alias CubDB.Btree

  @type writer_result :: {Btree.t(), term} | {:cancel, term}
  @type writer_fun :: (Btree.t() -> writer_result)

  @spec acquire(GenServer.server(), writer_fun) :: term

  def acquire(db, fun) do
    btree = GenServer.call(db, :start_write, :infinity)

    returned =
      try do
        fun.(btree)
      rescue
        exception ->
          GenServer.call(db, :cancel_write, :infinity)
          reraise(exception, __STACKTRACE__)
      catch
        :throw, value ->
          GenServer.call(db, :cancel_write, :infinity)
          throw(value)

        :exit, value ->
          GenServer.call(db, :cancel_write, :infinity)
          exit(value)
      end

    case returned do
      {%Btree{} = btree, result} ->
        GenServer.call(db, {:complete_write, btree}, :infinity)
        result

      {:cancel, result} ->
        GenServer.call(db, :cancel_write, :infinity)
        result
    end
  end
end
