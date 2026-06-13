defmodule OffBroadwayKlife.ProducerPipelineTest do
  # Drives a real Broadway pipeline (no broker) through OffBroadwayKlife.Producer
  # with a fake Klife consumer group. This is the level that catches mistakes the
  # unit tests can't — e.g. partition_by must return an integer or the dispatcher
  # crashes on the first message.
  use ExUnit.Case, async: false

  alias Broadway.Message

  defmodule FakeConsumerGroup do
    # Hands out one batch of records on {"topic", 0}, then reports empty.
    def start_link(_args), do: Agent.start_link(fn -> false end, name: __MODULE__)

    def assigned_partitions(_group), do: [{"topic", 0}]

    def pull(_group, "topic", 0) do
      delivered? = Agent.get_and_update(__MODULE__, fn delivered -> {delivered, true} end)

      if delivered? do
        {:ok, :empty}
      else
        records =
          for offset <- 0..2 do
            %Klife.Record{
              topic: "topic",
              partition: 0,
              offset: offset,
              value: "value-#{offset}",
              key: "key-#{offset}",
              timestamp: 1_700_000_000_000 + offset,
              headers: [%{key: "h1", value: "v1"}]
            }
          end

        {:ok, records}
      end
    end

    def pull(_group, _topic, _partition), do: {:ok, :empty}

    def commit(_group, _topic, _partition, _offset), do: :ok
  end

  defmodule Pipeline do
    use Broadway

    def start_link(opts), do: Broadway.start_link(__MODULE__, opts)

    @impl true
    def handle_message(_processor, message, %{test: test}) do
      send(test, {:message, message})
      message
    end

    @impl true
    def handle_batch(_batcher, messages, batch_info, %{test: test}) do
      send(test, {:batch, batch_info, messages})
      messages
    end
  end

  defp start_pipeline(producer_extra) do
    producer_opts =
      [
        consumer_group: FakeConsumerGroup,
        group_name: "g",
        topics: [[name: "topic"]],
        receive_interval: 50
      ] ++ producer_extra

    {:ok, pid} =
      Pipeline.start_link(
        name: __MODULE__.Running,
        context: %{test: self()},
        producer: [module: {OffBroadwayKlife.Producer, producer_opts}],
        processors: [default: [concurrency: 2]],
        batchers: [default: [concurrency: 1, batch_size: 10, batch_timeout: 100]]
      )

    pid
  end

  defp receive_three_messages do
    for _ <- 0..2 do
      assert_receive {:message, %Message{} = message}, 2_000
      message
    end
  end

  test "default :klife format delivers the full Klife.Record as data" do
    pid = start_pipeline([])

    messages = receive_three_messages()
    records = Enum.map(messages, & &1.data)

    # Reaching handle_message proves partition_by returned an integer (a tuple
    # would crash the dispatcher). In :klife mode data is the record itself.
    assert Enum.all?(records, &match?(%Klife.Record{}, &1))
    # single partition -> one processor -> offset order; assert the exact order.
    assert Enum.map(records, & &1.value) == ["value-0", "value-1", "value-2"]
    # Everything lives on the record; metadata is empty and headers stay maps.
    assert Enum.all?(messages, &(&1.metadata == %{}))
    assert Enum.all?(records, &(&1.headers == [%{key: "h1", value: "v1"}]))

    # Routing/batching still work in :klife mode.
    assert Enum.all?(messages, &(&1.batch_key == {"topic", 0}))
    assert_receive {:batch, _batch_info, batch}, 2_000
    assert Enum.all?(batch, &match?(%Klife.Record{}, &1.data))

    Broadway.stop(pid)
  end

  test ":broadway_kafka format mirrors broadway_kafka's message shape" do
    pid = start_pipeline(message_format: :broadway_kafka)

    messages = receive_three_messages()

    # single partition -> one processor -> offset order; assert the exact order.
    assert Enum.map(messages, & &1.data) == ["value-0", "value-1", "value-2"]
    assert Enum.map(messages, & &1.metadata.offset) == [0, 1, 2]

    # Exact key set, ts populated, headers as {key, value} tuples (not Klife maps).
    message = Enum.find(messages, &(&1.metadata.offset == 0))

    assert Map.keys(message.metadata) |> Enum.sort() == [
             :headers,
             :key,
             :offset,
             :partition,
             :topic,
             :ts
           ]

    assert message.metadata.key == "key-0"
    assert message.metadata.ts == 1_700_000_000_000
    assert message.metadata.headers == [{"h1", "v1"}]
    assert Enum.all?(messages, &(&1.batch_key == {"topic", 0}))

    Broadway.stop(pid)
  end
end
