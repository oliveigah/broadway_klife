defmodule BroadwayKlife do
  @moduledoc """
  A [Broadway](https://hexdocs.pm/broadway) connector for Kafka built on
  [Klife](https://hexdocs.pm/klife).

  It bridges Klife's consumer group *manual mode* with Broadway: Klife handles
  membership (KIP-848), heartbeats, rebalances, fetching and buffering, while
  Broadway handles concurrency, batching, retries and acknowledgement.

  The only public piece is `BroadwayKlife.Producer` - the Broadway producer you
  plug into a pipeline's `:producer` option. The consumer group it drives is a
  plain `Klife.Consumer.ConsumerGroup`:

      defmodule MyApp.KafkaConsumerGroup do
        use Klife.Consumer.ConsumerGroup, client: MyApp.KafkaClient
      end

  See `BroadwayKlife.Producer` for a full example.
  """
end
