defmodule OffBroadwayKlife.Producer do
  @producer_opts [
    client: [
      type: {:custom, __MODULE__, :validate_client, []},
      doc:
        "A module that `use`s `Klife.Client`. The producer starts `OffBroadwayKlife.ConsumerGroup` " <>
          "on this client (a single group membership) and drives it in manual mode. All " <>
          "`:client` pipelines in a node must use the same client — see " <>
          "`OffBroadwayKlife.ConsumerGroup`. Exactly one of `:client` or `:consumer_group` " <>
          "must be set."
    ],
    consumer_group: [
      type: {:custom, __MODULE__, :validate_consumer_group, []},
      doc:
        "A module that `use`s `Klife.Consumer.ConsumerGroup`, as an alternative to " <>
          "`:client` for when you also want the group's lifecycle callbacks. Started " <>
          "once by the producer (a single group membership) and driven in manual mode."
    ],
    group_name: [
      type: :string,
      required: true,
      doc: "The Kafka consumer group name."
    ],
    topics: [
      type: {:list, :keyword_list},
      required: true,
      doc:
        "List of `Klife.Consumer.ConsumerGroup.TopicConfig` keyword lists, e.g. " <>
          "`[[name: \"orders\"], [name: \"events\", fetch_max_bytes: 500_000]]`. " <>
          "Every topic is forced into `mode: :manual`."
    ],
    receive_interval: [
      type: :non_neg_integer,
      default: 1_000,
      doc: "Milliseconds to wait before polling Klife again after a poll returned no records."
    ],
    message_format: [
      type: {:in, [:klife, :broadway_kafka]},
      default: :klife,
      doc: "Shape of the emitted `Broadway.Message`s. See the \"Message format\" section below."
    ],
    fetch_strategy: [
      type: :any,
      doc: "Forwarded to the consumer group. See `Klife.Consumer.ConsumerGroup`."
    ],
    committers_count: [
      type: :pos_integer,
      doc: "Forwarded to the consumer group. See `Klife.Consumer.ConsumerGroup`."
    ],
    default_topic_config: [
      type: :keyword_list,
      doc: "Forwarded to the consumer group. See `Klife.Consumer.ConsumerGroup`."
    ],
    instance_id: [
      type: :string,
      doc:
        "Forwarded to the consumer group (static membership). See `Klife.Consumer.ConsumerGroup`."
    ],
    rebalance_timeout_ms: [
      type: :non_neg_integer,
      doc: "Forwarded to the consumer group. See `Klife.Consumer.ConsumerGroup`."
    ]
  ]

  @moduledoc """
  A Broadway producer that consumes from Kafka through
  [Klife](https://hexdocs.pm/klife)'s consumer group *manual mode*.

  Klife runs the full consumer-group machinery, heartbeats, rebalances, fetching and buffering,
  while this producer drives delivery: it `pull`s buffered batches on demand, turns each `Klife.Record`
  into a `Broadway.Message`, and `commit`s offsets back as Broadway acknowledges them.

  ## Usage

  Define a `Klife.Client` (see Klife's docs for client configuration) and make
  sure it is started in your supervision tree *before* the pipeline:

      defmodule MyApp.KafkaClient do
        use Klife.Client, otp_app: :my_app
      end

  Then start Broadway with this producer, pointing it at the client:

      defmodule MyApp.Pipeline do
        use Broadway

        def start_link(_opts) do
          Broadway.start_link(__MODULE__,
            name: __MODULE__,
            producer: [
              module:
                {OffBroadwayKlife.Producer,
                 client: MyApp.KafkaClient,
                 group_name: "my-broadway-group",
                 topics: [[name: "orders"], [name: "events"]],
                 receive_interval: 500},
              concurrency: 1
            ],
            processors: [default: [concurrency: 10]]
          )
        end

        @impl true
        def handle_message(_processor, message, _context) do
          IO.inspect(message.data)
          message
        end
      end

  The producer starts its built-in consumer group, `OffBroadwayKlife.ConsumerGroup`,
  on the client and supervises it as part of the pipeline, so there is no
  consumer group to define or add to your supervision tree. The built-in group
  is bound to the client on first start, which means all `:client` pipelines in
  a node must use the same Klife client (see `OffBroadwayKlife.ConsumerGroup`).

  ### Using a consumer group module

  To run Klife's consumer lifecycle callbacks (`handle_consumer_start/3` and
  `handle_consumer_stop/4`) alongside Broadway — or to consume through more
  than one Klife client — define a consumer group module and pass it as
  `:consumer_group` instead of `:client` (the two options are mutually
  exclusive):

      defmodule MyApp.KafkaConsumerGroup do
        use Klife.Consumer.ConsumerGroup, client: MyApp.KafkaClient

        @impl true
        def handle_consumer_start(_topic, _partition, _group_name) do
          # e.g. emit telemetry
          :ok
        end
      end

      producer: [
        module:
          {OffBroadwayKlife.Producer,
           consumer_group: MyApp.KafkaConsumerGroup,
           group_name: "my-broadway-group",
           topics: [[name: "orders"]]}
      ]

  Do not implement `handle_record_batch/4`: Broadway drives fetching and
  committing through Klife's manual mode, so that callback never runs. The
  producer starts the group in either case — do not add it to your supervision
  tree.

  ## Options

  #{NimbleOptions.docs(@producer_opts)}

  ## Message format

  `:message_format` controls the shape of each `Broadway.Message`:

  - `:klife` (default) - `message.data` is the full `Klife.Record` struct, the
    same type used across Klife's produce and fetch APIs. It carries everything
    (value, key, headers as maps, `timestamp`, `consumer_attempts`,
    `batch_attributes`, ...), and `message.metadata` is empty. Prefer this for
    new pipelines: it is lossless and consistent with the rest of Klife.

        def handle_message(_, %{data: %Klife.Record{value: value}} = msg, _),
          do: msg

  - `:broadway_kafka` - `message.data` is the raw value and `message.metadata`
    mirrors [broadway_kafka](https://hexdocs.pm/broadway_kafka): `%{topic,
    partition, offset, key, ts, headers}` with headers as `{key, value}` tuples.
    Use this to drop `OffBroadwayKlife.Producer` into an existing broadway_kafka
    pipeline without changing `handle_message/3`.

  Either way, routing, batching, acknowledgement and offset commits are
  identical — only the user-facing message shape changes.

  ## Producer concurrency

  The Klife consumer group is started once and keeps a single membership no
  matter how many producers run. You may set producer `concurrency > 1`: each
  assigned `{topic, partition}` is claimed by exactly one producer via a stable
  hash, so producers share the pull/commit load with no overlap and no extra group
  members with no coordinator process overhead. For maximum concurrency raise it
  up to the expected number of assigned partitions for a given member of the group,
  any exceeding producer will stay idle.

  Raise `concurrency` up to the number of partitions you expect to be
  assigned to the application; producers beyond assigned stay idle.

  ## Ordering

  Kafka guarantees ordering per topic-partition. The connector preserves it end
  to end: each partition is pulled by exactly one producer, which emits its
  records in offset order, and it sets Broadway's `:partition_by` so that every
  record of a given `{topic, partition}` is always routed to the same processor
  (and batcher) stage. Records of different partitions still process
  concurrently.

  When you use a batcher, each batch also holds a single partition's records:
  the connector defaults `:batch_key` to `{topic, partition}`, so a
  `handle_batch/4` call maps to one partition's contiguous offset range.

  You may override `:batch_key` (or `:batcher`) in `handle_message/3`:

  - *Coarsening* it (e.g. to the topic, or leaving it `:default`) packs records
    from several partitions into one `handle_batch` call, giving fuller batches
    and fewer round-trips when a node owns many low-volume partitions.
    Per-partition ordering is still preserved — the batch just interleaves
    partitions, so group by `partition` inside `handle_batch` if your logic
    needs to.
  - *Refining* it (a sub-partition key) or routing to multiple `:batcher`s
    re-splits a partition across independently-flushing batches, which **gives
    up strict per-partition ordering**. Use it only when per-key (not
    per-partition) ordering is enough, or to fan message types out to different
    sinks (e.g. a dead-letter batcher).

  Offset commits stay correct in every case — `OffBroadwayKlife.OffsetTracker`
  handles out-of-order acks regardless of how records are batched.

  Because the connector manages `:partition_by`, you must not set it yourself —
  doing so raises. Scale out across partitions with processor/batcher
  concurrency.

  ## Delivery semantics

  Klife provides at-least-once delivery and this producer preserves it: an
  offset is only committed once it and every lower delivered offset on the
  same partition* have been acknowledged by Broadway (see
  `OffBroadwayKlife.OffsetTracker`).

  Because Kafka tracks a single committed offset per partition, a failed
  message cannot be skipped while committing past it. Both successful and
  failed messages therefore advance the offset; handle failures explicitly via
  `c:Broadway.handle_failed/2` (for example, by producing to a dead-letter
  topic) rather than relying on them blocking the partition.
  """

  use GenStage

  require Logger

  alias Broadway.Message
  alias OffBroadwayKlife.OffsetTracker

  @behaviour Broadway.Producer
  @behaviour Broadway.Acknowledger

  # Subset of validated options that are forwarded to the consumer group's
  # start_link/1 (Klife validates them in full there).
  @consumer_group_passthrough [
    :instance_id,
    :rebalance_timeout_ms,
    :fetch_strategy,
    :committers_count,
    :default_topic_config
  ]

  @impl Broadway.Producer
  def prepare_for_start(_module, broadway_opts) do
    {producer_module, producer_opts} = broadway_opts[:producer][:module]
    opts = NimbleOptions.validate!(producer_opts, @producer_opts)
    {cg_mod, client_args} = resolve_cg!(opts)

    cg_args =
      [group_name: opts[:group_name], topics: force_manual_mode(opts[:topics])] ++
        client_args ++ Keyword.take(opts, @consumer_group_passthrough)

    cg_child = %{
      id: {cg_mod, opts[:group_name]},
      start: {cg_mod, :start_link, [cg_args]},
      restart: :permanent,
      type: :worker
    }

    # Each producer needs the pool size to claim its share of partitions (owns?/3);
    # carry the validated opts (defaults applied, :consumer_group resolved)
    # forward to init/1.
    producer_count = broadway_opts[:producer][:concurrency] || 1

    init_opts =
      opts
      |> Keyword.put(:producer_count, producer_count)
      |> Keyword.put(:consumer_group, cg_mod)

    producer_config =
      Keyword.put(broadway_opts[:producer], :module, {producer_module, init_opts})

    broadway_opts =
      broadway_opts
      |> Keyword.put(:producer, producer_config)
      |> put_partition_by()

    {[cg_child], broadway_opts}
  end

  @impl GenStage
  def init(opts) do
    broadway = Keyword.fetch!(opts, :broadway)

    state = %{
      consumer_group: Keyword.fetch!(opts, :consumer_group),
      group_name: Keyword.fetch!(opts, :group_name),
      receive_interval: Keyword.fetch!(opts, :receive_interval),
      message_format: Keyword.fetch!(opts, :message_format),
      producer_index: Keyword.fetch!(broadway, :index),
      producer_count: Keyword.fetch!(opts, :producer_count),
      demand: 0,
      receive_timer: nil,
      ack_ref: self(),
      offset_tracker: OffsetTracker.new()
    }

    {:producer, state}
  end

  @impl GenStage
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl GenStage
  def handle_info(:receive_messages, %{receive_timer: nil} = state) do
    {:noreply, [], state}
  end

  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  def handle_info({__MODULE__, :processed, tp_offsets}, state) do
    {tracker, commits} = OffsetTracker.done(state.offset_tracker, tp_offsets)

    Enum.each(commits, fn {{topic, partition}, offset} ->
      commit(state, topic, partition, offset)
    end)

    {:noreply, [], %{state | offset_tracker: tracker}}
  end

  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  @impl Broadway.Acknowledger
  def ack(ack_ref, successful, failed) do
    # Both successful and failed messages advance the partition offset; see the
    # moduledoc for why. Failures are reported via handle_failed/2 upstream.
    tp_offsets = Enum.map(successful ++ failed, &message_tp_offset/1)
    send(ack_ref, {__MODULE__, :processed, tp_offsets})
    :ok
  end

  ## Internal

  defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    {messages, state} = receive_messages_from_klife(state, demand)
    {:noreply, messages, state}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp receive_messages_from_klife(state, total_demand) do
    # pull_round makes a single in-order pass and stops once demand is met, so
    # without shuffling the partitions at the head of the list would always be
    # served first and the tail could starve. A fresh shuffle each round keeps
    # fetching fair across the assigned partitions.
    partitions = state |> assigned_partitions() |> Enum.shuffle()
    {messages, tracker} = pull_round(partitions, total_demand, state, [], state.offset_tracker)
    new_demand = max(total_demand - length(messages), 0)

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        {_, _} -> schedule_receive_messages(0)
      end

    {messages,
     %{state | demand: new_demand, offset_tracker: tracker, receive_timer: receive_timer}}
  end

  # The pulls are sequential on purpose. `pull/3` is a local GenServer.call that
  # returns already-buffered records (not a network fetch), so it's cheap, and we
  # stop as soon as demand is met, so the number of calls scales with demand, not
  # with the assigned partition count. Combined with the shuffle in
  # receive_messages_from_klife/2, no partition starves. Parallelizing the pulls
  # would not be worth the complexity hit.
  #
  # If a single producer pumping many partitions ever becomes the bottleneck, raise producer
  # `concurrency`: the allocator shards partitions across producers, cutting each
  # one's pull count and parallelizing across processes within GenStage's model.
  defp pull_round(_partitions, demand, _state, messages, tracker) when demand <= 0,
    do: {messages, tracker}

  defp pull_round([], _demand, _state, messages, tracker), do: {messages, tracker}

  defp pull_round([tp | rest], demand, state, messages, tracker) do
    case pull(state, tp) do
      {:ok, [_ | _] = records} ->
        new_messages = Enum.map(records, &build_message(&1, state.ack_ref, state.message_format))
        tracker = OffsetTracker.delivered(tracker, tp, Enum.map(records, & &1.offset))
        pull_round(rest, demand - length(new_messages), state, messages ++ new_messages, tracker)

      _empty_or_error ->
        pull_round(rest, demand, state, messages, tracker)
    end
  end

  defp assigned_partitions(state) do
    state.consumer_group.assigned_partitions(state.group_name)
    |> Enum.filter(&owns?(&1, state.producer_index, state.producer_count))
  catch
    kind, reason when kind in [:exit, :error] ->
      Logger.warning(
        "OffBroadwayKlife assigned_partitions failed for #{state.group_name}: #{inspect({kind, reason})}"
      )

      []
  end

  @doc false
  def owns?(topic_partition, producer_index, producer_count) do
    :erlang.phash2(topic_partition, producer_count) == producer_index
  end

  defp pull(state, {topic, partition}) do
    state.consumer_group.pull(state.group_name, topic, partition)
  catch
    :exit, reason ->
      Logger.warning("OffBroadwayKlife pull exited for #{topic}:#{partition}: #{inspect(reason)}")

      {:ok, :empty}
  end

  defp commit(state, topic, partition, offset) do
    state.consumer_group.commit(state.group_name, topic, partition, offset)
  catch
    :exit, reason ->
      Logger.warning(
        "OffBroadwayKlife commit exited for #{topic}:#{partition}@#{offset}: #{inspect(reason)}"
      )

      :ok
  end

  defp build_message(%Klife.Record{} = record, ack_ref, :klife) do
    %Message{
      data: record,
      metadata: %{},
      acknowledger: {__MODULE__, ack_ref, {record.topic, record.partition, record.offset}},
      batch_key: {record.topic, record.partition}
    }
  end

  defp build_message(%Klife.Record{} = record, ack_ref, :broadway_kafka) do
    %Message{
      data: record.value,
      metadata: %{
        topic: record.topic,
        partition: record.partition,
        offset: record.offset,
        key: record.key,
        ts: record.timestamp,
        headers: encode_headers(record.headers)
      },
      acknowledger: {__MODULE__, ack_ref, {record.topic, record.partition, record.offset}},
      batch_key: {record.topic, record.partition}
    }
  end

  defp encode_headers(nil), do: []
  defp encode_headers(headers), do: Enum.map(headers, fn %{key: k, value: v} -> {k, v} end)

  defp message_tp_offset(%Message{acknowledger: {_module, _ack_ref, {topic, partition, offset}}}) do
    {{topic, partition}, offset}
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end

  defp force_manual_mode(topics) when is_list(topics) do
    Enum.map(topics, fn topic_config -> Keyword.put(topic_config, :mode, :manual) end)
  end

  defp put_partition_by(broadway_opts) do
    if partition_by_set?(broadway_opts) do
      raise ArgumentError,
            "OffBroadwayKlife.Producer manages :partition_by to preserve Kafka per-partition " <>
              "ordering and it must not be set manually. Remove the :partition_by option."
    end

    fun = &__MODULE__.partition_by/1

    broadway_opts = Keyword.update!(broadway_opts, :processors, &put_stage_partition_by(&1, fun))

    case Keyword.fetch(broadway_opts, :batchers) do
      {:ok, batchers} ->
        Keyword.put(broadway_opts, :batchers, put_stage_partition_by(batchers, fun))

      :error ->
        broadway_opts
    end
  end

  defp put_stage_partition_by(stages, fun) do
    Enum.map(stages, fn {name, config} -> {name, Keyword.put(config, :partition_by, fun)} end)
  end

  defp partition_by_set?(broadway_opts) do
    Keyword.has_key?(broadway_opts, :partition_by) or
      stage_has_partition_by?(broadway_opts[:processors]) or
      stage_has_partition_by?(broadway_opts[:batchers])
  end

  defp stage_has_partition_by?(nil), do: false

  defp stage_has_partition_by?(stages) do
    Enum.any?(stages, fn {_name, config} -> Keyword.has_key?(config, :partition_by) end)
  end

  @doc false
  def partition_by(%Message{data: %Klife.Record{topic: topic, partition: partition}}) do
    :erlang.phash2({topic, partition})
  end

  def partition_by(%Message{metadata: %{topic: topic, partition: partition}}) do
    :erlang.phash2({topic, partition})
  end

  # NimbleOptions cannot express "exactly one of"; both options are optional in
  # the schema and the pairing is enforced here. Returns the consumer group
  # module plus the extra start args it needs: with :client the stock
  # OffBroadwayKlife.ConsumerGroup module is used and gets bound to the client on
  # its first start (see its moduledoc); a :consumer_group module already
  # carries its client in its `use` options.
  defp resolve_cg!(opts) do
    case {Keyword.fetch(opts, :client), Keyword.fetch(opts, :consumer_group)} do
      {{:ok, client}, :error} ->
        {OffBroadwayKlife.ConsumerGroup, [client: client]}

      {:error, {:ok, mod}} ->
        {mod, []}

      {:error, :error} ->
        raise ArgumentError,
              "one of :client or :consumer_group is required in OffBroadwayKlife.Producer options"

      {{:ok, _}, {:ok, _}} ->
        raise ArgumentError,
              ":client and :consumer_group are mutually exclusive in OffBroadwayKlife.Producer " <>
                "options; set only one of them"
    end
  end

  @doc false
  def validate_client(mod) when is_atom(mod) and not is_nil(mod) do
    if Code.ensure_loaded?(mod) and function_exported?(mod, :get_default_fetcher, 0) do
      {:ok, mod}
    else
      {:error,
       "#{inspect(mod)} is not a Klife client; define it with " <>
         "`use Klife.Client, otp_app: :my_app`"}
    end
  end

  def validate_client(other) do
    {:error, "expected a Klife client module, got: #{inspect(other)}"}
  end

  @doc false
  def validate_consumer_group(mod) when is_atom(mod) do
    exports? =
      Code.ensure_loaded?(mod) and
        function_exported?(mod, :assigned_partitions, 1) and
        function_exported?(mod, :pull, 3) and
        function_exported?(mod, :commit, 4)

    if exports? do
      {:ok, mod}
    else
      {:error,
       "#{inspect(mod)} is not a Klife consumer group; define it with " <>
         "`use Klife.Consumer.ConsumerGroup, client: MyClient`"}
    end
  end

  def validate_consumer_group(other) do
    {:error, "expected a consumer group module, got: #{inspect(other)}"}
  end
end
