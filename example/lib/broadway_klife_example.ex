defmodule BroadwayKlifeExample do
  def produce(value \\ nil) do
    val = value || :rand.bytes(10) |> Base.encode16()
    BroadwayKlifeExample.KafkaClient.produce(%Klife.Record{topic: "orders", value: val})
  end
end
