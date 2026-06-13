defmodule BroadwayKlifeExample.MixProject do
  use Mix.Project

  def project do
    [
      app: :broadway_klife_example,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {BroadwayKlifeExample.Application, []}
    ]
  end

  defp deps do
    [
      {:broadway_klife, path: ".."},
      {:klife, path: "../../klife"}
    ]
  end
end
