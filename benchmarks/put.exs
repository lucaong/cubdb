data_dir = "tmp/bm_put"

cleanup = fn ->
  with {:ok, files} <- File.ls(data_dir) do
    for file <- files, do: File.rm(Path.join(data_dir, file))
    File.rmdir(data_dir)
  end
end

small = "small value"
{:ok, one_mb} = File.read("benchmarks/data/1mb")
{:ok, ten_mb} = File.read("benchmarks/data/10mb")

Benchee.run(
  %{
    "CubDB.put/3" => fn {key, value, db} ->
      CubDB.put(db, key, value)
    end
  },
  inputs: %{
    "small value" => small,
    "1MB value" => one_mb,
    "10MB value" => ten_mb 
  },
  before_scenario: fn input ->
    cleanup.()
    {:ok, db} = CubDB.start_link(data_dir)
    {input, db}
  end,
  before_each: fn {input, db} ->
    key = :rand.uniform(10_000)
    {key, input, db}
  end,
  after_scenario: fn {input, db} ->
    IO.puts("#{CubDB.size(db)} entries written to database.")
    cleanup.()
  end
)
