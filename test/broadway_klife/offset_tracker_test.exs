defmodule BroadwayKlife.OffsetTrackerTest do
  use ExUnit.Case, async: true

  alias BroadwayKlife.OffsetTracker

  @tp {"topic", 0}

  test "commits the last offset of a fully-acked contiguous batch" do
    tracker = OffsetTracker.new() |> OffsetTracker.delivered(@tp, [10, 11, 12])

    {_tracker, commits} = OffsetTracker.done(tracker, [{@tp, 10}, {@tp, 11}, {@tp, 12}])

    assert commits == [{@tp, 12}]
  end

  test "holds back higher offsets until the contiguous prefix is acked" do
    tracker = OffsetTracker.new() |> OffsetTracker.delivered(@tp, [10, 11, 12])

    # Ack 12 first: 10 is still the front of the queue, so nothing drains.
    {tracker, commits} = OffsetTracker.done(tracker, [{@tp, 12}])
    assert commits == []

    # Ack 10 (the first delivered offset): it has nothing before it, so it
    # commits immediately. 11 is still missing, so 12 stays held back.
    {tracker, commits} = OffsetTracker.done(tracker, [{@tp, 10}])
    assert commits == [{@tp, 10}]

    # Once 11 lands, 11 and the already-acked 12 drain together; commit 12.
    {_tracker, commits} = OffsetTracker.done(tracker, [{@tp, 11}])
    assert commits == [{@tp, 12}]
  end

  test "advances incrementally as the prefix grows" do
    tracker = OffsetTracker.new() |> OffsetTracker.delivered(@tp, [1, 2, 3, 4])

    {tracker, commits} = OffsetTracker.done(tracker, [{@tp, 1}])
    assert commits == [{@tp, 1}]

    {tracker, commits} = OffsetTracker.done(tracker, [{@tp, 3}])
    assert commits == []

    {_tracker, commits} = OffsetTracker.done(tracker, [{@tp, 2}])
    assert commits == [{@tp, 3}]
  end

  test "handles non-contiguous offsets (gaps from compaction/transactions)" do
    # Offsets 6 and 8 exist, 7 does not (e.g. a transaction marker was filtered).
    tracker = OffsetTracker.new() |> OffsetTracker.delivered(@tp, [6, 8])

    {tracker, commits} = OffsetTracker.done(tracker, [{@tp, 6}])
    assert commits == [{@tp, 6}]

    {_tracker, commits} = OffsetTracker.done(tracker, [{@tp, 8}])
    assert commits == [{@tp, 8}]
  end

  test "tracks partitions independently" do
    tp_a = {"topic", 0}
    tp_b = {"topic", 1}

    tracker =
      OffsetTracker.new()
      |> OffsetTracker.delivered(tp_a, [100, 101])
      |> OffsetTracker.delivered(tp_b, [200, 201])

    {_tracker, commits} = OffsetTracker.done(tracker, [{tp_a, 100}, {tp_b, 200}, {tp_b, 201}])

    assert Enum.sort(commits) == [{tp_a, 100}, {tp_b, 201}]
  end

  test "delivered batches accumulate across pulls" do
    tracker =
      OffsetTracker.new()
      |> OffsetTracker.delivered(@tp, [1, 2])
      |> OffsetTracker.delivered(@tp, [3, 4])

    {_tracker, commits} =
      OffsetTracker.done(tracker, [{@tp, 1}, {@tp, 2}, {@tp, 3}, {@tp, 4}])

    assert commits == [{@tp, 4}]
  end

  test "ignores acks for unknown partitions" do
    {_tracker, commits} = OffsetTracker.done(OffsetTracker.new(), [{@tp, 5}])
    assert commits == []
  end
end
