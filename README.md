[![ci](https://github.com/oliveigah/broadway_klife/actions/workflows/ci.yml/badge.svg)](https://github.com/oliveigah/broadway_klife/actions/workflows/ci.yml)
[![hex.pm badge](https://img.shields.io/badge/Package%20on%20hex.pm-informational)](https://hex.pm/packages/broadway_klife)
[![Documentation badge](https://img.shields.io/badge/Documentation-ff69b4)](https://hexdocs.pm/broadway_klife)

# BroadwayKlife

A [Broadway](https://hexdocs.pm/broadway) connector for Apache Kafka, built on
[Klife](https://hexdocs.pm/klife).

It bridges Klife's consumer group **manual mode** with Broadway.

## Installation

```elixir
def deps do
  [
    {:broadway_klife, "~> 0.1.0"},
    {:klife, "~> 1.1"}
  ]
end
```

## Usage

1. Configure and start a `Klife.Client` (see the
   [Klife docs](https://hexdocs.pm/klife)) — this is your Kafka connection.

2. Define the Broadway pipeline, pointing the producer at the client:

   ```elixir
   defmodule MyApp.Pipeline do
     use Broadway

     def start_link(_opts) do
       Broadway.start_link(__MODULE__,
         name: __MODULE__,
         producer: [
           module:
             {BroadwayKlife.Producer,
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
       IO.inspect(message.data, label: "got record")
       message
     end
   end
   ```

3. Start the client before the pipeline in your supervision tree. The producer
   starts its built-in Klife consumer group on the client and supervises it as
   part of the pipeline — there is no consumer group to define or add yourself.

   ```elixir
   children = [
     MyApp.KafkaClient,
     MyApp.Pipeline
   ]
   ```

## Message format

`:message_format` controls the shape of each `Broadway.Message`:

- **`:klife`** (default) — `message.data` is the full `Klife.Record`, the same
  struct used across Klife's produce/fetch APIs (value, key, map headers,
  `timestamp`, `consumer_attempts`, `batch_attributes`, …). `message.metadata`
  is empty. Lossless and consistent with the rest of Klife — prefer it for new
  pipelines.

- **`:broadway_kafka`** — `message.data` is the raw value and `message.metadata`
  mirrors [broadway_kafka](https://hexdocs.pm/broadway_kafka):
  `%{topic, partition, offset, key, ts, headers}` with headers as `{key, value}`
  tuples. Drop `BroadwayKlife.Producer` with this message format into an existing
  broadway_kafka pipeline without touching `handle_message/3`.

Routing, batching, acknowledgement and offset commits are identical either way
only the user-facing message shape changes.

## Delivery semantics

Klife is at-least-once, and this connector preserves it. An offset is committed
only once it and every lower delivered offset on the same partition have
been acknowledged by Broadway, so out-of-order acks never advance the committed
offset past an unprocessed record.

Because Kafka tracks a single committed offset per partition, a failed message
cannot be skipped while committing past it. **Both successful and failed messages
advance the offset**; handle failures explicitly via `c:Broadway.handle_failed/2`
(e.g. a dead-letter topic) rather than relying on them to block the partition.

```elixir
@impl true
def handle_failed(messages, _context) do
  Enum.each(messages, fn %Broadway.Message{data: record} ->
    # Send record to DLQ
  end)
  messages
end
```

## Producer concurrency

The Klife consumer group is started once and keeps a single membership no
matter how many producers run. You may set producer `concurrency > 1`: each
assigned `{topic, partition}` is claimed by exactly one producer via a stable
hash, so producers share the pull/commit load with no overlap and no extra group
members with no coordinator process overhead. For maximum concurrency raise it
up to the expected number of assigned partitions for a given member of the group,
any exceeding producer will stay idle.

## Ordering

Kafka orders records per topic-partition, and the connector preserves that
ordering end to end. Each partition is pulled by exactly one producer, which
emits it in offset order, and the connector sets Broadway's `:partition_by` so
that all records of a given `{topic, partition}` are always handled by the same
processor (and batcher) stage. Different partitions are still processed
concurrently.
