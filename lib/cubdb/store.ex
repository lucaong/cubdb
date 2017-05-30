defprotocol CubDB.Store do
  @type node_type :: atom
  @type location :: non_neg_integer
  @type btree_node :: {node_type, any}

  @spec put_node(any, btree_node) :: location
  def put_node(store, node)

  @spec get_node(any, location) :: btree_node | {:error, String.t}
  def get_node(store, location)

  @spec get_latest_header(any) :: {location, btree_node} | nil
  def get_latest_header(store)
end
