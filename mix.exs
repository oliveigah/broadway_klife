defmodule BroadwayKlife.MixProject do
  use Mix.Project

  def project do
    [
      app: :broadway_klife,
      version: "0.1.0",
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Broadway Klife",
      description: "A broadway connector for kafka using Klife",
      source_url: "https://github.com/oliveigah/broadway_klife",
      extras: [],
      package: [
        licenses: ["MIT"],
        links: %{"GitHub" => "https://github.com/oliveigah/broadway_klife"}
      ],
      docs: [
        main: "readme",
        extras: [
          "README.md"
        ]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:klife, "~> 1.1"},
      {:broadway, "~> 1.3"},
      {:nimble_options, "~> 1.0"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end
end
