defmodule BroadwayKlife.TestConsumerGroup do
  @moduledoc false
  use Klife.Consumer.ConsumerGroup, client: BroadwayKlife.TestClient
end
