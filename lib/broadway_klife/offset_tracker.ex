defmodule OffBroadwayKlife.OffsetTracker do
  @moduledoc false

  defstruct partitions: %{}

  def new, do: %__MODULE__{}

  def delivered(%__MODULE__{} = tracker, _tp, []), do: tracker

  def delivered(%__MODULE__{} = tracker, tp, offsets) do
    update_in(tracker.partitions[tp], fn
      nil -> %{delivered: :queue.from_list(offsets), done: MapSet.new()}
      %{delivered: queue} = state -> %{state | delivered: enqueue_all(queue, offsets)}
    end)
  end

  def done(%__MODULE__{} = tracker, tp_offsets) do
    tp_offsets
    |> Enum.group_by(fn {tp, _offset} -> tp end, fn {_tp, offset} -> offset end)
    |> Enum.reduce({tracker, []}, fn {tp, offsets}, {acc_tracker, commits} ->
      case acc_tracker.partitions[tp] do
        nil ->
          {acc_tracker, commits}

        %{delivered: queue, done: done} ->
          done = Enum.reduce(offsets, done, &MapSet.put(&2, &1))
          {new_queue, new_done, commit} = drain(queue, done, :none)
          new_state = %{delivered: new_queue, done: new_done}
          acc_tracker = put_in(acc_tracker.partitions[tp], new_state)

          case commit do
            :none -> {acc_tracker, commits}
            offset -> {acc_tracker, [{tp, offset} | commits]}
          end
      end
    end)
  end

  defp enqueue_all(queue, offsets), do: Enum.reduce(offsets, queue, &:queue.in/2)

  defp drain(queue, done, last_committable) do
    case :queue.peek(queue) do
      {:value, offset} ->
        if MapSet.member?(done, offset) do
          {_, rest} = :queue.out(queue)
          drain(rest, MapSet.delete(done, offset), offset)
        else
          {queue, done, last_committable}
        end

      :empty ->
        {queue, done, last_committable}
    end
  end
end
