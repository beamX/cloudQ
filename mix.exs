defmodule Cloudq.Mixfile do
  use Mix.Project

  def project do
    [app: :cloudQ,
     version: "0.1.0",
     elixir: "~> 1.4",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps]
  end

  def application do
    [applications: [:observer,:runtime_tools,:wx,:erlcloud,:poolboy,:lager,:hackney,:jch,:brod], mod: {:cloudQ_app, []}]
  end

  defp deps do
    [
      {:erlcloud, "~> 2.2"},
      {:poolboy, "~> 1.5"},
      {:lager, "~> 3.4"},
      {:hackney, "~> 1.8"},
      {:jch, "~> 0.2.3"},
      {:brod, "~> 2.5"},
      {:distillery, "~> 1.4", runtime: false}
    ]
  end
end
