defmodule BroadwayKlife.ConsumerGroupStarter do
  @moduledoc false

  # Klife registers one consumer group process per {client, module, group name},
  # so when a pipeline is restarted under the same group name (crash + supervisor
  # restart), the new instance can race the old one's leave handshake and get
  # {:error, {:already_started, pid}}. Wait for the old instance to go away
  # instead of failing the whole Broadway start; past the deadline the error is
  # returned as-is, so a genuinely duplicated group configuration still fails.
  @takeover_timeout 5_000

  def start_link(cg_mod, cg_args) do
    deadline = System.monotonic_time(:millisecond) + @takeover_timeout
    do_start_link(cg_mod, cg_args, deadline)
  end

  defp do_start_link(cg_mod, cg_args, deadline) do
    with {:error, {:already_started, old_pid}} <- cg_mod.start_link(cg_args) do
      remaining = deadline - System.monotonic_time(:millisecond)
      ref = Process.monitor(old_pid)

      receive do
        {:DOWN, ^ref, :process, ^old_pid, _reason} ->
          # The registry may clear the old entry slightly after the DOWN.
          Process.sleep(10)
          do_start_link(cg_mod, cg_args, deadline)
      after
        max(remaining, 0) ->
          Process.demonitor(ref, [:flush])
          {:error, {:already_started, old_pid}}
      end
    end
  end
end
