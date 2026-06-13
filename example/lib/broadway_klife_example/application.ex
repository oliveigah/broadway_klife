defmodule BroadwayKlifeExample.Application do
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
      Supervisor.start_link([], strategy: :one_for_one, name: BroadwayKlifeExample.Supervisor)

    {:ok, _} = Supervisor.start_child(sup, BroadwayKlifeExample.KafkaClient)

    :ok = Klife.Utils.create_topics(BroadwayKlifeExample.KafkaClient, @topics)

    {:ok, _} = Supervisor.start_child(sup, BroadwayKlifeExample.Pipeline)

    {:ok, sup}
  end
end
