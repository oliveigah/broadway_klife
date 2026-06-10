defmodule BroadwayKlife.TestKafka do
  @moduledoc false

  alias KlifeProtocol.Messages.CreateTopics

  @bootstrap "localhost:19092"

  @spec create_topic(String.t(), pos_integer(), pos_integer()) :: :ok
  def create_topic(name, partitions, replication_factor \\ 2) do
    {:ok, conn} = Klife.Connection.new(@bootstrap, false, [], [], [])

    {:ok, %{brokers: brokers, controller: controller_id}} =
      Klife.Connection.Controller.get_cluster_info(conn)

    {_id, controller_url} = Enum.find(brokers, fn {id, _url} -> id == controller_id end)
    {:ok, controller_conn} = Klife.Connection.new(controller_url, false, [], [], [])

    content = %{
      topics: [
        %{
          name: name,
          num_partitions: partitions,
          replication_factor: replication_factor,
          assignments: [],
          configs: []
        }
      ],
      timeout_ms: 15_000,
      validate_only: false
    }

    :ok =
      %{content: content, headers: %{correlation_id: 1}}
      |> CreateTopics.serialize_request(2)
      |> Klife.Connection.write(controller_conn)

    {:ok, data} = Klife.Connection.read(controller_conn)
    {:ok, %{content: resp}} = CreateTopics.deserialize_response(data, 2)

    # 0 = ok, 36 = TOPIC_ALREADY_EXISTS
    case Enum.filter(resp.topics, fn topic -> topic.error_code not in [0, 36] end) do
      [] -> :ok
      errors -> raise "create_topic failed: #{inspect(errors)}"
    end
  end
end
