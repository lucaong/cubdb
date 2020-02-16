data_dir = "tmp/bm_put"

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

Benchee.run(
  %{
    "CubDB.put/3" => fn {key, value, db} ->
      CubDB.put(db, key, value)
    end
  },
  inputs: %{
    "small value, auto sync" => {small, [auto_compact: false, auto_file_sync: true]},
    "small value" => {small, [auto_compact: false, auto_file_sync: false]},
    "1KB value" => {one_kb, [auto_compact: false, auto_file_sync: false]},
    "1MB value" => {one_mb, [auto_compact: false, auto_file_sync: false]},
    "10MB value" => {ten_mb, [auto_compact: false, auto_file_sync: false]}
  },
  before_scenario: fn {value, options} ->
    cleanup.()
    {:ok, db} = CubDB.start_link(data_dir, options)
    {value, db}
  end,
  before_each: fn {value, db} ->
    key = :rand.uniform(10_000)
    {key, value, db}
  end,
  after_scenario: fn {_value, db} ->
    IO.puts("#{CubDB.size(db)} entries written to database.")
    CubDB.stop(db)
    cleanup.()
  end
)
