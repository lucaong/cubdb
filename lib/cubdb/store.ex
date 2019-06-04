defprotocol CubDB.Store do
  @type btree_node_type :: atom
  @type location :: non_neg_integer
  @type btree_node :: {btree_node_type, list({any, non_neg_integer})}
  @type btree_header :: {non_neg_integer, non_neg_integer}

  @spec put_node(Store.t(), btree_node) :: location
  def put_node(store, node)

  @spec put_header(Store.t(), btree_header) :: location
  def put_header(store, header)

  @spec commit(Store.t()) :: :ok | {:error, String.t()}
  def commit(store)

  @spec get_node(Store.t(), location) :: btree_node | {:error, String.t()}
  def get_node(store, location)

  @spec get_latest_header(Store.t()) :: {location, btree_header} | nil
  def get_latest_header(store)

  @spec close(Store.t()) :: :ok | {:error, String.t()}
  def close(store)

  @spec blank?(Store.t()) :: boolean
  def blank?(store)
end
