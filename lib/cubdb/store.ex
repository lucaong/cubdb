defprotocol CubDB.Store do
  @type node_type :: atom
  @type location :: non_neg_integer
  @type btree_node :: {node_type, list(tuple)}
  @type btree_header :: {node_type, tuple}

  @spec put_node(any, btree_node) :: location
  def put_node(store, node)

  @spec put_header(any, btree_header) :: location
  def put_header(store, header)

  @spec commit(any) :: :ok | {:error, String.t}
  def commit(store)

  @spec get_node(any, location) :: btree_node | {:error, String.t}
  def get_node(store, location)

  @spec get_latest_header(any) :: {location, btree_node} | nil
  def get_latest_header(store)
end
