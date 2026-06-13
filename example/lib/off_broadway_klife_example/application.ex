defmodule OffBroadwayKlifeExample.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @topics [
    %{name: "orders", partitions: 3}
  ]

  @impl true
  def start(_type, _args) do
    {:ok, sup} =
      Supervisor.start_link([], strategy: :one_for_one, name: OffBroadwayKlifeExample.Supervisor)

    {:ok, _} = Supervisor.start_child(sup, OffBroadwayKlifeExample.KafkaClient)

    :ok = Klife.Utils.create_topics(OffBroadwayKlifeExample.KafkaClient, @topics)

    {:ok, _} = Supervisor.start_child(sup, OffBroadwayKlifeExample.Pipeline)

    {:ok, sup}
  end
end
