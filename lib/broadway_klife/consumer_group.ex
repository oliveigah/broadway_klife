defmodule BroadwayKlife.ConsumerGroup do
  @moduledoc """
  The Klife consumer group module used by pipelines configured with `:client`.

  It defines no callback — Broadway drives fetching and committing through
  Klife's manual mode — and no compile-time client: the first pipeline start
  binds it to the configured `Klife.Client` through Klife's client binding
  (see "The client binding" in `Klife.Consumer.ConsumerGroup`).

  Because that binding is per module and permanent, every `:client` pipeline
  in a node must use the same Klife client — starting one with a different
  client raises. To consume through more than one client, define a consumer
  group module per client and pass it via the producer's `:consumer_group`
  option instead.
  """

  use Klife.Consumer.ConsumerGroup
end
