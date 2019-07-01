defprotocol CubDB.Store do
  @moduledoc false

  alias CubDB.Btree

  @spec put_node(t, Btree.btree_node()) :: Btree.location()
  def put_node(store, node)

  @spec put_header(t, Btree.btree_header()) :: Btree.location()
  def put_header(store, header)

  @spec sync(t) :: :ok | {:error, String.t()}
  def sync(store)

  @spec get_node(t, Btree.location()) :: Btree.btree_node() | {:error, String.t()}
  def get_node(store, location)

  @spec get_latest_header(t) :: {Btree.location(), Btree.btree_header()} | nil
  def get_latest_header(store)

  @spec close(t) :: :ok | {:error, String.t()}
  def close(store)

  @spec blank?(t) :: boolean
  def blank?(store)
end
