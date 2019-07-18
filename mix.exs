defmodule CubDB.Mixfile do
  use Mix.Project

  def project do
    [
      app: :cubdb,
      version: "0.10.0",
      elixir: "~> 1.6",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      elixirc_paths: elixirc_paths(Mix.env),
      deps: deps(),
      package: package(),
      source_url: "https://github.com/lucaong/cubdb",
      docs: [
        main: "CubDB"
      ],
      dialyzer: [ignore_warnings: "dialyzer_ignore.exs"],
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test, "coveralls.detail": :test, "coveralls.post": :test, "coveralls.html": :test]
    ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    # Specify extra applications you'll use from Erlang/Elixir
    [extra_applications: [:logger]]
  end

  # Dependencies can be Hex packages:
  #
  #   {:my_dep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:my_dep, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [
      {:quixir, "~> 0.9", only: :test},
      {:dialyxir, "~> 1.0.0-rc.6", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.20", only: :dev, runtime: false},
      {:benchee, "~> 1.0", only: :dev},
      {:excoveralls, "~> 0.10", only: :test}
    ]
  end

  defp package() do
    [
      description: "A pure-Elixir embedded key-value database",
      files: ["lib", "LICENSE", "mix.exs"],
      maintainers: ["Luca Ongaro"],
      licenses: ["Apache-2.0"],
      links: %{}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/shared_examples/"]
  defp elixirc_paths(_), do: ["lib"]
end
