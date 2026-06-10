defmodule BroadwayKlife.ProducerTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  alias BroadwayKlife.Producer

  # Minimal stand-in for a `use Klife.Consumer.ConsumerGroup` module: it only
  # needs to export the functions the producer relies on.
  defmodule FakeConsumerGroup do
    def assigned_partitions(_group_name), do: []
    def pull(_group_name, _topic, _partition), do: {:ok, :empty}
    def commit(_group_name, _topic, _partition, _offset), do: :ok
    def start_link(_args), do: :ignore
  end

  defp broadway_opts(extra \\ []) do
    [
      name: __MODULE__.Pipeline,
      producer: [
        module:
          {Producer,
           consumer_group: FakeConsumerGroup, group_name: "g", topics: [[name: "orders"]]},
        concurrency: 1
      ],
      processors: [default: []]
    ] ++ extra
  end

  defp message_for(topic, partition) do
    %Message{
      data: "payload",
      metadata: %{topic: topic, partition: partition, offset: 0},
      acknowledger: Broadway.NoopAcknowledger.init()
    }
  end

  describe "partition_by/1" do
    test "returns a non-negative integer (Broadway routes via rem/2)" do
      result = Producer.partition_by(message_for("orders", 7))

      assert is_integer(result) and result >= 0
      assert result == :erlang.phash2({"orders", 7})
    end

    test "is stable per {topic, partition} and differs across them" do
      assert Producer.partition_by(message_for("orders", 7)) ==
               Producer.partition_by(message_for("orders", 7))

      refute Producer.partition_by(message_for("orders", 7)) ==
               Producer.partition_by(message_for("orders", 8))
    end

    test "reads {topic, partition} from the record in :klife mode (data is a record)" do
      message = %Message{
        data: %Klife.Record{topic: "orders", partition: 7, offset: 0},
        metadata: %{},
        acknowledger: Broadway.NoopAcknowledger.init()
      }

      assert Producer.partition_by(message) == :erlang.phash2({"orders", 7})
    end
  end

  describe "prepare_for_start/2 ordering guarantee" do
    test "injects partition_by into each processor config (not top level)" do
      {[_cg_child], updated} = Producer.prepare_for_start(__MODULE__, broadway_opts())

      # Top-level is too late (Broadway carries it over before prepare_for_start),
      # so it must land on the processor config itself.
      refute Keyword.has_key?(updated, :partition_by)
      assert updated[:processors][:default][:partition_by] == (&Producer.partition_by/1)
    end

    test "raises if partition_by is set manually" do
      assert_raise ArgumentError, ~r/:partition_by/, fn ->
        Producer.prepare_for_start(__MODULE__, broadway_opts(partition_by: fn _ -> :x end))
      end
    end

    test "raises on an unknown :message_format" do
      opts = broadway_opts()
      {Producer, producer_opts} = opts[:producer][:module]
      producer_opts = Keyword.put(producer_opts, :message_format, :json)
      opts = put_in(opts[:producer][:module], {Producer, producer_opts})

      assert_raise NimbleOptions.ValidationError, ~r/:message_format/, fn ->
        Producer.prepare_for_start(__MODULE__, opts)
      end
    end

    test "raises when :consumer_group is not a Klife consumer group" do
      opts = broadway_opts()
      {Producer, producer_opts} = opts[:producer][:module]
      producer_opts = Keyword.put(producer_opts, :consumer_group, Enum)
      opts = put_in(opts[:producer][:module], {Producer, producer_opts})

      assert_raise NimbleOptions.ValidationError, ~r/consumer group/, fn ->
        Producer.prepare_for_start(__MODULE__, opts)
      end
    end

    test "leaves producer concurrency as set (no longer forced to 1)" do
      opts = put_in(broadway_opts()[:producer][:concurrency], 8)
      {[_cg_child], updated} = Producer.prepare_for_start(__MODULE__, opts)

      assert updated[:producer][:concurrency] == 8
    end

    test "injects the producer pool size into the producer options" do
      opts = put_in(broadway_opts()[:producer][:concurrency], 4)
      {[_cg_child], updated} = Producer.prepare_for_start(__MODULE__, opts)
      {_module, producer_opts} = updated[:producer][:module]

      assert producer_opts[:producer_count] == 4
    end

    test "defaults the pool size to 1 when concurrency is unset" do
      opts = broadway_opts()
      producer = Keyword.delete(opts[:producer], :concurrency)
      opts = Keyword.put(opts, :producer, producer)

      {[_cg_child], updated} = Producer.prepare_for_start(__MODULE__, opts)
      {_module, producer_opts} = updated[:producer][:module]

      assert producer_opts[:producer_count] == 1
    end
  end

  describe "init/1" do
    test "reads this producer's index and the pool size" do
      # init/1 receives options already validated by prepare_for_start (defaults
      # applied), so receive_interval/message_format are present.
      opts = [
        consumer_group: FakeConsumerGroup,
        group_name: "g",
        receive_interval: 1_000,
        message_format: :klife,
        producer_count: 3,
        broadway: [index: 2, name: __MODULE__.Pipeline]
      ]

      {:producer, state} = Producer.init(opts)

      assert state.producer_index == 2
      assert state.producer_count == 3
      assert state.message_format == :klife
      assert state.consumer_group == FakeConsumerGroup
    end
  end

  describe "owns?/3 (partition allocation)" do
    test "every assigned partition is owned by exactly one producer in the pool" do
      partitions = for t <- ["orders", "events"], p <- 0..9, do: {t, p}
      count = 4

      owners =
        Enum.map(partitions, fn tp ->
          owning = for i <- 0..(count - 1), Producer.owns?(tp, i, count), do: i
          assert length(owning) == 1, "#{inspect(tp)} owned by #{inspect(owning)}"
          hd(owning)
        end)

      # the load is actually spread, not dumped on one producer
      assert owners |> Enum.uniq() |> length() > 1
    end

    test "allocation is stable for a given partition and pool size" do
      assert Producer.owns?({"orders", 3}, 1, 4) == Producer.owns?({"orders", 3}, 1, 4)
    end

    test "a single producer owns every partition" do
      for tp <- [{"orders", 0}, {"orders", 1}, {"events", 5}] do
        assert Producer.owns?(tp, 0, 1)
      end
    end
  end
end
