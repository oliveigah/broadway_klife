defmodule BroadwayKlife.IntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Broadway.Message
  alias Klife.Record

  defmodule Pipeline do
    use Broadway

    alias Broadway.Message
    alias Klife.Record

    def start_link(opts), do: Broadway.start_link(__MODULE__, opts)

    @impl true
    def handle_message(_processor, %Message{} = message, %{test: test} = context) do
      block_ms = Map.get(context, :block_ms, 10_000)

      case msg_action(message) do
        "fail" -> Message.failed(message, :injected_failure)
        "block" -> Process.sleep(block_ms) && message
        _ -> send(test, {:message, message}) && message
      end
    end

    @impl true
    def handle_batch(_batcher, messages, batch_info, %{test: test} = context) do
      block_ms = Map.fetch!(context, :block_ms)

      messages =
        Enum.map(messages, fn message ->
          case batch_action(message) do
            "fail" -> Message.failed(message, :injected_failure)
            "block" -> Process.sleep(block_ms) && message
            _ -> message
          end
        end)

      send(test, {:batch, batch_info, messages})
      messages
    end

    @impl true
    def handle_failed(messages, %{test: test}) do
      Enum.each(messages, fn message -> send(test, {:failed, message}) end)
      messages
    end

    defp msg_action(%Message{data: %Record{headers: headers}}),
      do: find_header(headers, "msg_action")

    defp msg_action(%Message{metadata: %{headers: headers}}),
      do: find_header(headers, "msg_action")

    defp msg_action(_message), do: nil

    defp batch_action(%Message{data: %Record{headers: headers}}),
      do: find_header(headers, "batch_action")

    defp batch_action(%Message{metadata: %{headers: headers}}),
      do: find_header(headers, "batch_action")

    defp batch_action(_message), do: nil

    defp find_header(headers, key) do
      Enum.find_value(headers, fn
        %{key: ^key, value: value} -> value
        {^key, value} -> value
        _ -> nil
      end)
    end
  end

  setup_all do
    start_supervised!(BroadwayKlife.TestClient)
    :ok
  end

  setup do
    t1 = "bk_e2e_#{:rand.bytes(8) |> Base.encode16()}"
    t2 = "bk_e2e_#{:rand.bytes(8) |> Base.encode16()}"
    t3 = "bk_e2e_#{:rand.bytes(8) |> Base.encode16()}"

    :ok =
      Klife.Utils.create_topics(BroadwayKlife.TestClient, [
        %{name: t1, partitions: 3},
        %{name: t2, partitions: 3},
        %{name: t3, partitions: 3}
      ])

    tp_list =
      for t <- [t1, t2, t3],
          p <- 0..2 do
        {t, p}
      end

    %{tp_list: tp_list}
  end

  defp tp_list_to_topics(tp_list) do
    tp_list
    |> Enum.map(fn {t, _p} -> t end)
    |> Enum.uniq()
  end

  test "consumes produced records end to end (:klife format)", %{tp_list: tp_list} do
    start_pipeline(topics: tp_list_to_topics(tp_list))
    values = for i <- 1..10, do: "v-#{i}"
    produce!(tp_list, values)

    expected_records = length(tp_list) * 10

    consumed =
      for _ <- 1..expected_records do
        assert_receive {:message, %Message{data: %Record{} = record}}, 15_000
        record
      end

    grouped_consumed =
      Enum.group_by(consumed, fn %Record{} = rec -> {rec.topic, rec.partition} end)

    Enum.each(grouped_consumed, fn {{t, p}, rec_list} ->
      assert {t, p} in tp_list
      rec_values = Enum.map(rec_list, fn %Record{topic: ^t, partition: ^p} = rec -> rec.value end)
      assert rec_values == values
    end)
  end

  test "consumer_group: module escape hatch consumes end to end", %{tp_list: tp_list} do
    start_pipeline(
      topics: tp_list_to_topics(tp_list),
      cg_source: [consumer_group: BroadwayKlife.TestConsumerGroup]
    )

    values = for i <- 1..3, do: "cg-#{i}"
    produce!(tp_list, values)

    expected_records = length(tp_list) * 3

    consumed =
      for _ <- 1..expected_records do
        assert_receive {:message, %Message{data: %Record{} = record}}, 15_000
        record
      end

    grouped = Enum.group_by(consumed, fn %Record{} = rec -> {rec.topic, rec.partition} end)

    Enum.each(grouped, fn {{t, p}, rec_list} ->
      assert {t, p} in tp_list
      assert Enum.map(rec_list, & &1.value) == values
    end)
  end

  test "consumes with :broadway_kafka format (data is the value + metadata)", %{tp_list: tp_list} do
    start_pipeline(topics: tp_list_to_topics(tp_list), message_format: :broadway_kafka)
    values = for i <- 1..5, do: "bk-#{i}"
    produce!(tp_list, values)

    expected_records = length(tp_list) * 5

    consumed =
      for _ <- 1..expected_records do
        assert_receive {:message, %Message{data: data, metadata: meta}}, 15_000
        assert is_binary(data)
        assert {meta.topic, meta.partition} in tp_list
        assert is_integer(meta.offset)
        assert is_integer(meta.ts)
        {{meta.topic, meta.partition}, data}
      end

    consumed
    |> Enum.group_by(fn {tp, _v} -> tp end, fn {_tp, v} -> v end)
    |> Enum.each(fn {tp, vals} ->
      assert tp in tp_list
      assert vals == values
    end)
  end

  test "producer concurrency > 1 shards partitions with no loss or duplication",
       %{tp_list: tp_list} do
    values = for i <- 1..10, do: "c-#{i}"
    produce!(tp_list, values)

    start_pipeline(
      topics: tp_list_to_topics(tp_list),
      offset_reset_policy: :earliest,
      producer_concurrency: 2
    )

    expected_records = length(tp_list) * 10

    consumed =
      for _ <- 1..expected_records do
        assert_receive {:message, %Message{data: %Record{} = record}}, 15_000
        record
      end

    # no extra/duplicate deliveries
    refute_receive {:message, _}, 1_000

    grouped = Enum.group_by(consumed, fn %Record{} = r -> {r.topic, r.partition} end)
    assert map_size(grouped) == length(tp_list)

    Enum.each(grouped, fn {tp, rec_list} ->
      assert tp in tp_list
      assert Enum.map(rec_list, & &1.value) == values
    end)
  end

  test "resumes from the committed offset after a restart (no re-delivery)", %{tp_list: tp_list} do
    topics = tp_list_to_topics(tp_list)
    group = "bk_e2e_resume_#{:rand.bytes(8) |> Base.encode16()}"
    per_tp = 5
    total = length(tp_list) * per_tp

    batch1 = for i <- 1..per_tp, do: "b1-#{i}"
    produce!(tp_list, batch1)

    start_pipeline(
      topics: topics,
      group_name: group,
      offset_reset_policy: :earliest,
      id: :run1
    )

    for _ <- 1..total, do: assert_receive({:message, %Message{}}, 15_000)
    # graceful stop flushes commits via the revoke handshake
    :ok = stop_supervised(:run1)

    batch2 = for i <- 1..per_tp, do: "b2-#{i}"
    produce!(tp_list, batch2)

    refute_receive {:message, _}, 1_000

    start_pipeline(
      topics: topics,
      group_name: group,
      offset_reset_policy: :earliest,
      id: :run2
    )

    consumed =
      for _ <- 1..total do
        assert_receive {:message, %Message{data: %Record{} = record}}, 15_000
        record
      end

    refute_receive {:message, _}, 1_000

    grouped = Enum.group_by(consumed, fn %Record{} = r -> {r.topic, r.partition} end)
    assert map_size(grouped) == length(tp_list)

    Enum.each(grouped, fn {tp, rec_list} ->
      assert tp in tp_list
      assert Enum.map(rec_list, & &1.value) == batch2
    end)
  end

  test "delivers per-partition batches (handle_batch, batch_key = {topic, partition})",
       %{tp_list: tp_list} do
    per_tp = 13
    batch_size = 5
    total = length(tp_list) * per_tp
    values = for i <- 1..per_tp, do: "batch-#{i}"
    produce!(tp_list, values)

    start_pipeline(
      topics: tp_list_to_topics(tp_list),
      offset_reset_policy: :earliest,
      batchers: [default: [concurrency: 2, batch_size: batch_size, batch_timeout: 200]]
    )

    expected_batches = length(tp_list) * ceil(per_tp / batch_size)

    batches =
      Enum.map(1..expected_batches, fn _ ->
        assert_receive {:batch, batch_info, messages}, 15_000
        {batch_info.batch_key, messages}
      end)
      |> Enum.group_by(fn {k, _msgs} -> k end, fn {_k, msgs} -> msgs end)

    consumed =
      Enum.flat_map(batches, fn {_k, batch_list} ->
        Enum.flat_map(batch_list, fn batch -> batch end)
      end)

    assert length(consumed) == total

    Enum.each(batches, fn {{t, p}, batch_list} ->
      assert length(batch_list) == ceil(per_tp / batch_size)

      Enum.with_index(batch_list, 1)
      |> Enum.each(fn {b, idx} ->
        if idx == length(batch_list) do
          expected_length_for_last =
            case rem(per_tp, batch_size) do
              0 -> batch_size
              other -> other
            end

          assert length(b) == expected_length_for_last
        else
          assert length(b) == batch_size
        end
      end)

      received_records = List.flatten(batch_list) |> Enum.map(fn msg -> msg.data end)

      assert received_records ==
               Enum.sort(received_records, fn rec1, rec2 -> rec1.offset < rec2.offset end)

      received_values =
        received_records
        |> Enum.map(fn %Record{} = rec ->
          assert rec.topic == t
          assert rec.partition == p
          rec.value
        end)

      assert values == received_values
    end)
  end

  test "failed messages hit handle_failed, advance the offset, and are not re-delivered",
       %{tp_list: tp_list} do
    topics = tp_list_to_topics(tp_list)
    group = "bk_e2e_fail_#{:rand.bytes(8) |> Base.encode16()}"

    produce_actions!(tp_list, [
      {"ok-1", "ok", "ok"},
      {"boom", "fail", "invalid"},
      {"ok-2", "ok", "ok"}
    ])

    start_pipeline(
      topics: topics,
      group_name: group,
      offset_reset_policy: :earliest,
      id: :fail1
    )

    oks =
      for _ <- 1..(length(tp_list) * 2) do
        assert_receive {:message, %Message{data: %Record{} = record}}, 15_000
        record
      end

    fails =
      for _ <- 1..length(tp_list) do
        assert_receive {:failed, %Message{data: %Record{} = record}}, 15_000
        record
      end

    oks
    |> Enum.group_by(fn %Record{} = r -> {r.topic, r.partition} end)
    |> Enum.each(fn {tp, recs} ->
      assert tp in tp_list
      assert Enum.map(recs, & &1.value) == ["ok-1", "ok-2"]
    end)

    fails
    |> Enum.group_by(fn %Record{} = r -> {r.topic, r.partition} end)
    |> Enum.each(fn {tp, recs} ->
      assert tp in tp_list
      assert Enum.map(recs, & &1.value) == ["boom"]
    end)

    :ok = stop_supervised(:fail1)

    start_pipeline(
      topics: topics,
      group_name: group,
      offset_reset_policy: :earliest,
      id: :fail2
    )

    refute_receive {:message, _}, 2_000
    refute_receive {:failed, _}, 2_000
  end

  test "offset tracker holds the commit at the contiguous prefix on out-of-order acks",
       %{tp_list: tp_list} do
    {topic, partition} = hd(tp_list)
    group = "bk_e2e_tracker_#{:rand.bytes(8) |> Base.encode16()}"

    produce_actions!([{topic, partition}], [
      {"r0", "ok", "block"},
      {"r1", "fail", "invalid"},
      {"r2", "ok", "block"},
      {"r3", "fail", "invalid"}
    ])

    pipe1_pid =
      start_pipeline(
        topics: [topic],
        group_name: group,
        offset_reset_policy: :earliest,
        batchers: [default: [concurrency: 1, batch_size: 100, batch_timeout: 5_000]],
        id: :pipe1
      )

    assert_receive {:message, %Message{data: %Record{value: "r0"}}}, 15_000
    assert_receive {:failed, %Message{data: %Record{value: "r1"}}}, 15_000
    assert_receive {:message, %Message{data: %Record{value: "r2"}}}, 15_000
    assert_receive {:failed, %Message{data: %Record{value: "r3"}}}, 15_000

    Process.exit(pipe1_pid, :kill)

    refute_receive {:batch, _batch_info, _messages}, 2_000

    start_pipeline(
      topics: [topic],
      group_name: group,
      offset_reset_policy: :earliest,
      batchers: [default: [concurrency: 1, batch_size: 100, batch_timeout: 5_000]],
      block_ms: 0,
      id: :pipe2
    )

    assert_receive {:message, %Message{data: %Record{value: "r0"}}}, 15_000
    assert_receive {:failed, %Message{data: %Record{value: "r1"}}}, 15_000
    assert_receive {:message, %Message{data: %Record{value: "r2"}}}, 15_000
    assert_receive {:failed, %Message{data: %Record{value: "r3"}}}, 15_000

    assert_receive {:batch, _batch_info, messages}, 15_000

    assert ["r0", "r2"] = Enum.map(messages, fn msg -> msg.data.value end)
    :ok = stop_supervised(:pipe2)

    start_pipeline(
      topics: [topic],
      group_name: group,
      offset_reset_policy: :earliest,
      batchers: [default: [concurrency: 1, batch_size: 100, batch_timeout: 5_000]],
      block_ms: 0,
      id: :pipe3
    )

    refute_receive {:message, _}, 5_000
    refute_receive {:failed, _}
    refute_receive {:batch, _batch_info, _messages}
  end

  test "producer crash redelivers checked-out records instead of skipping them",
       %{tp_list: tp_list} do
    {topic, partition} = hd(tp_list)
    unique = :rand.bytes(8) |> Base.encode16()
    group = "bk_e2e_pkill_#{unique}"
    pipeline_name = :"bk_e2e_pkill_pipe_#{unique}"

    # k0 is processed and acked normally; k1 blocks its processor with k2
    # queued behind it (same partition, same processor), so offsets 1..2 are
    # checked out by the producer but redelivered when it is killed.
    produce_actions!([{topic, partition}], [
      {"k0", "ok", "ok"},
      {"k1", "block", "ok"},
      {"k2", "ok", "ok"}
    ])

    start_pipeline(
      topics: [topic],
      group_name: group,
      offset_reset_policy: :earliest,
      name: pipeline_name,
      block_ms: 1_500,
      id: :pkill
    )

    assert_receive {:message, %Message{data: %Record{value: "k0"}}}, 15_000

    [producer_name] = Broadway.producer_names(pipeline_name)
    producer_pid = Process.whereis(producer_name)
    assert is_pid(producer_pid)
    Process.exit(producer_pid, :kill)

    # The first k2 is the copy already in flight when the producer died (its
    # ack goes to the dead pid). The second one proves the consumer detected
    # the dead puller and redelivered the checked-out window to the restarted
    # producer instead of letting commits skip past it.
    assert_receive {:message, %Message{data: %Record{value: "k2"}}}, 15_000
    assert_receive {:message, %Message{data: %Record{value: "k2"}}}, 15_000
  end

  test "pulls more records than demand", %{tp_list: tp_list} do
    start_pipeline(topics: tp_list_to_topics(tp_list), processor_concurrency: 1, max_demand: 5)
    values = for i <- 1..10, do: "v-#{i}"
    produce!(tp_list, values)

    expected_records = length(tp_list) * 10

    consumed =
      for _ <- 1..expected_records do
        assert_receive {:message, %Message{data: %Record{} = record}}, 5_000
        record
      end

    grouped_consumed =
      Enum.group_by(consumed, fn %Record{} = rec -> {rec.topic, rec.partition} end)

    Enum.each(grouped_consumed, fn {{t, p}, rec_list} ->
      assert {t, p} in tp_list
      rec_values = Enum.map(rec_list, fn %Record{topic: ^t, partition: ^p} = rec -> rec.value end)
      assert rec_values == values
    end)
  end

  defp produce!(tp_list, values) do
    records =
      for {t, p} <- tp_list,
          v <- values do
        %Record{topic: t, partition: p, value: v}
      end

    results = BroadwayKlife.TestClient.produce_batch(records)
    assert {:ok, _} = Record.verify_batch(results)
  end

  defp produce_actions!(tp_list, specs) do
    records =
      for {t, p} <- tp_list, {value, msg_action, batch_action} <- specs do
        headers = [
          %{key: "msg_action", value: msg_action},
          %{key: "batch_action", value: batch_action}
        ]

        %Record{topic: t, partition: p, value: value, headers: headers}
      end

    results = BroadwayKlife.TestClient.produce_batch(records)
    assert {:ok, _} = Record.verify_batch(results)
  end

  defp start_pipeline(opts) do
    topics = Keyword.fetch!(opts, :topics)
    unique = :rand.bytes(8) |> Base.encode16()
    reset = Keyword.get(opts, :offset_reset_policy, :earliest)

    producer_extra =
      opts
      |> Keyword.take([:message_format])
      |> Keyword.merge(Keyword.get(opts, :producer_opts, []))

    cg_source = Keyword.get(opts, :cg_source, client: BroadwayKlife.TestClient)

    producer_opts =
      cg_source ++
        [
          group_name: Keyword.get(opts, :group_name, "bk_e2e_grp_#{unique}"),
          topics: Enum.map(topics, fn t -> [name: t, offset_reset_policy: reset] end),
          receive_interval: 100
        ] ++ producer_extra

    broadway =
      [
        name: Keyword.get(opts, :name, :"bk_e2e_pipeline_#{unique}"),
        context: %{
          test: self(),
          block_ms: Keyword.get(opts, :block_ms, 10_000)
        },
        producer: [
          module: {BroadwayKlife.Producer, producer_opts},
          concurrency: Keyword.get(opts, :producer_concurrency, 1)
        ],
        processors: [
          default: [
            concurrency: Keyword.get(opts, :processor_concurrency, 4),
            max_demand: Keyword.get(opts, :max_demand, 100)
          ]
        ]
      ]
      |> maybe_put_batchers(Keyword.get(opts, :batchers))

    start_supervised!({Pipeline, broadway},
      id: Keyword.get(opts, :id, :"pipeline_#{unique}"),
      restart: :temporary
    )
  end

  defp maybe_put_batchers(broadway, nil), do: broadway
  defp maybe_put_batchers(broadway, batchers), do: Keyword.put(broadway, :batchers, batchers)
end
