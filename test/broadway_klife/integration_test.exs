defmodule BroadwayKlife.IntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Broadway.Message
  alias Klife.Record

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
    start_pipeline(tp_list_to_topics(tp_list), [])
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

  test "consumes with :broadway_kafka format (data is the value + metadata)", %{tp_list: tp_list} do
    start_pipeline(tp_list_to_topics(tp_list), message_format: :broadway_kafka)
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

    start_pipeline_opts(
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

    start_pipeline_opts(
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

    # run1 is fully stopped: nothing consumes batch2 until run2 starts. If the old
    # pipeline were still alive it would (already assigned) deliver batch2 here.
    refute_receive {:message, _}, 1_000

    # same group: a committed offset exists, so it must resume past batch1
    start_pipeline_opts(
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

    # every partition resumes past batch1 and delivers exactly batch2, in order —
    # no sorting, so re-delivery or reordering would fail the assertion.
    grouped = Enum.group_by(consumed, fn %Record{} = r -> {r.topic, r.partition} end)
    assert map_size(grouped) == length(tp_list)

    Enum.each(grouped, fn {tp, rec_list} ->
      assert tp in tp_list
      assert Enum.map(rec_list, & &1.value) == batch2
    end)
  end

  test "delivers per-partition batches (handle_batch, batch_key = {topic, partition})",
       %{tp_list: tp_list} do
    per_tp = 4
    total = length(tp_list) * per_tp
    values = for i <- 1..per_tp, do: "batch-#{i}"
    produce!(tp_list, values)

    start_pipeline_opts(
      topics: tp_list_to_topics(tp_list),
      offset_reset_policy: :earliest,
      batchers: [default: [concurrency: 2, batch_size: 100, batch_timeout: 200]]
    )

    batches = collect_batches([], total)
    consumed = Enum.flat_map(batches, fn {_info, msgs} -> msgs end)
    assert length(consumed) == total

    # each batch holds exactly one partition's records
    Enum.each(batches, fn {_info, msgs} ->
      partitions =
        msgs
        |> Enum.map(fn %Message{data: %Record{} = r} -> {r.topic, r.partition} end)
        |> Enum.uniq()

      assert length(partitions) == 1
      assert hd(partitions) in tp_list
    end)
  end

  defp collect_batches(acc, remaining) when remaining <= 0, do: acc

  defp collect_batches(acc, remaining) do
    receive do
      {:batch, info, msgs} -> collect_batches([{info, msgs} | acc], remaining - length(msgs))
    after
      15_000 -> flunk("timed out waiting for batches, #{remaining} records still missing")
    end
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

  defp start_pipeline(topics, producer_extra) do
    unique = :rand.bytes(8) |> Base.encode16()

    producer_opts =
      [
        consumer_group: BroadwayKlife.TestConsumerGroup,
        group_name: "bk_e2e_grp_#{unique}",
        # :earliest (not :latest) so the consumer reads from offset 0 regardless
        # of whether records were produced before or after it joined — otherwise
        # a produce that races ahead of the group's position is silently skipped.
        topics: Enum.map(topics, fn t -> [name: t, offset_reset_policy: :earliest] end),
        receive_interval: 100
      ] ++ producer_extra

    start_supervised!(
      {Pipeline,
       name: :"bk_e2e_pipeline_#{unique}",
       context: %{test: self()},
       producer: [module: {BroadwayKlife.Producer, producer_opts}],
       processors: [default: [concurrency: 4]]}
    )
  end

  # Like start_pipeline/2 but with full control over offset policy, group name,
  # producer concurrency, batchers and the supervision id (for restart tests).
  defp start_pipeline_opts(opts) do
    topics = Keyword.fetch!(opts, :topics)
    unique = :rand.bytes(8) |> Base.encode16()
    reset = Keyword.get(opts, :offset_reset_policy, :earliest)

    producer_opts =
      [
        consumer_group: BroadwayKlife.TestConsumerGroup,
        group_name: Keyword.get(opts, :group_name, "bk_e2e_grp_#{unique}"),
        topics: Enum.map(topics, fn t -> [name: t, offset_reset_policy: reset] end),
        receive_interval: 100
      ] ++ Keyword.get(opts, :producer_opts, [])

    broadway =
      [
        name: :"bk_e2e_pipeline_#{unique}",
        context: %{test: self()},
        producer: [
          module: {BroadwayKlife.Producer, producer_opts},
          concurrency: Keyword.get(opts, :producer_concurrency, 1)
        ],
        processors: [default: [concurrency: 4]]
      ]
      |> maybe_put_batchers(Keyword.get(opts, :batchers))

    start_supervised!({Pipeline, broadway}, id: Keyword.get(opts, :id, :"pipeline_#{unique}"))
  end

  defp maybe_put_batchers(broadway, nil), do: broadway
  defp maybe_put_batchers(broadway, batchers), do: Keyword.put(broadway, :batchers, batchers)
end
