data_dir = "tmp/bm_get"

cleanup = fn ->
  with {:ok, files} <- File.ls(data_dir) do
    for file <- files, do: File.rm(Path.join(data_dir, file))
    File.rmdir(data_dir)
  end
end

small = "small value"
{:ok, one_kb} = File.read("benchmarks/data/1kb")
{:ok, one_mb} = File.read("benchmarks/data/1mb")
{:ok, ten_mb} = File.read("benchmarks/data/10mb")
n = 100

Benchee.run(
  %{
    "CubDB.get/3" => fn {key, db} ->
      CubDB.get(db, key)
    end
  },
  inputs: %{
    "small value" => small,
    "1KB value" => one_kb,
    "1MB value" => one_mb,
    "10MB value" => ten_mb
  },
  before_scenario: fn input ->
    cleanup.()
    {:ok, db} = CubDB.start_link(data_dir, auto_file_sync: false, auto_compact: false)
    for key <- 0..n, do: CubDB.put(db, key, input)
    db
  end,
  before_each: fn db ->
    key = :rand.uniform(n)
    {key, db}
  end,
  after_scenario: fn db ->
    CubDB.stop(db)
    cleanup.()
  end
)
