defmodule OffBroadwayKlife.TestConsumerGroup do
  @moduledoc false
  use Klife.Consumer.ConsumerGroup, client: OffBroadwayKlife.TestClient
end
