defmodule OffBroadwayKlifeExample.Pipeline do
  use Broadway

  alias Broadway.Message
  alias Klife.Record

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module:
          {OffBroadwayKlife.Producer,
           client: OffBroadwayKlifeExample.KafkaClient,
           group_name: "broadway-klife-example-group",
           topics: [[name: "orders", offset_reset_policy: :latest]],
           receive_interval: 500},
        concurrency: 1
      ],
      processors: [default: [concurrency: 2]]
    )
  end

  @impl true
  def handle_message(_processor, %Message{data: %Record{} = record} = message, _context) do
    IO.puts("""
    [Pipeline] Received record:
      topic:     #{record.topic}
      partition: #{record.partition}
      offset:    #{record.offset}
      key:       #{inspect(record.key)}
      value:     #{inspect(record.value)}
    """)

    message
  end
end
