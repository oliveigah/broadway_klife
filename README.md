# BroadwayKlife

A [Broadway](https://hexdocs.pm/broadway) connector for Apache Kafka, built on
[Klife](https://hexdocs.pm/klife).

It bridges Klife's consumer group **manual mode** with Broadway. Klife owns the
hard parts of being a Kafka consumer — KIP-848 group membership, heartbeats,
rebalances, fetching and buffering — while Broadway owns delivery concurrency,
batching, rate limiting, retries and acknowledgement.

> **Status:** early draft. The offset-tracking core is unit-tested; the
> producer ↔ broker path needs a running Kafka cluster to exercise.

## How it works

```
                Broadway topology
┌──────────────────────────────────────────────────────┐
│ BroadwayKlife.Producer  (GenStage producer +          │
│                          Broadway.Acknowledger)        │
│  • on demand, asks the group which partitions are      │
│    assigned here, then round-robin pulls batches       │
│  • Klife.Record -> Broadway.Message                    │
│  • on ack, commits the longest fully-acked offset      │
│    prefix per partition (BroadwayKlife.OffsetTracker)  │
└──────────────────────────────────────────────────────┘
   ▲ assigned_partitions/1      │ pull / commit
   │                            ▼
┌──────────────────────────────────────────────────────┐
│ use Klife.Consumer.ConsumerGroup  (mode: :manual,      │
│   forced by the producer)                              │
└──────────────────────────────────────────────────────┘
```

The producer never touches Kafka directly: it only calls `assigned_partitions/1`,
`pull/3` and `commit/4` on the consumer group. The consumer group is a plain
`Klife.Consumer.ConsumerGroup` — the producer boots it as a sibling child and
forces every topic into `mode: :manual`.

## Installation

```elixir
def deps do
  [
    {:broadway_klife, "~> 0.1.0"},
    {:klife, "~> 1.0"}
  ]
end
```

## Usage

1. Configure and start a `Klife.Client` (see the
   [Klife docs](https://hexdocs.pm/klife)) — this is your Kafka connection.

2. Define the consumer group module (a plain Klife consumer group):

   ```elixir
   defmodule MyApp.KafkaConsumerGroup do
     use Klife.Consumer.ConsumerGroup, client: MyApp.KafkaClient
   end
   ```

3. Define the Broadway pipeline:

   ```elixir
   defmodule MyApp.Pipeline do
     use Broadway

     def start_link(_opts) do
       Broadway.start_link(__MODULE__,
         name: __MODULE__,
         producer: [
           module:
             {BroadwayKlife.Producer,
              consumer_group: MyApp.KafkaConsumerGroup,
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

4. Start the client before the pipeline in your supervision tree:

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
  mirrors [broadway_kafka](https://hexdocs.pm/broadway_kafka): `%{topic,
  partition, offset, key, ts, headers}` with headers as `{key, value}` tuples.
  Drop `BroadwayKlife.Producer` into an existing broadway_kafka pipeline without
  touching `handle_message/3`.

Routing, batching, acknowledgement and offset commits are identical either way —
only the user-facing message shape changes.

## Options

See `BroadwayKlife.Producer` for the full list. The essentials:

| Option            | Required | Description                                                              |
| ----------------- | -------- | ------------------------------------------------------------------------ |
| `:consumer_group` | yes      | A module using `Klife.Consumer.ConsumerGroup`.                          |
| `:group_name`     | yes      | The Kafka consumer group name.                                           |
| `:topics`         | yes      | `Klife.Consumer.ConsumerGroup.TopicConfig` keyword lists (forced manual).|
| `:receive_interval` | no     | Backoff (ms) before re-polling after an empty pull. Default `1000`.      |
| `:message_format` | no       | `:klife` (default) or `:broadway_kafka`. See above.                      |

Other keys (`:fetch_strategy`, `:committers_count`, `:default_topic_config`,
`:instance_id`, `:rebalance_timeout_ms`) are forwarded to the consumer group.

## Delivery semantics

Klife is at-least-once, and this connector preserves it. An offset is committed
only once it **and every lower delivered offset on the same partition** have
been acknowledged by Broadway, so out-of-order acks never advance the committed
offset past an unprocessed record.

Because Kafka tracks a single committed offset per partition, a failed message
cannot be skipped while committing past it. Both successful and failed messages
advance the offset; handle failures explicitly via `c:Broadway.handle_failed/2`
(e.g. a dead-letter topic) rather than relying on them to block the partition.

## Producer concurrency

The Klife consumer group is started once and keeps a **single membership** no
matter how many producers run. You may set producer `concurrency > 1`: each
assigned `{topic, partition}` is claimed by exactly one producer via a stable
hash, so producers share the pull/commit load with no overlap and no extra group
members — and with no coordinator process, since every producer derives the same
disjoint view independently. Raise it up to the number of partitions the node is
assigned; producers beyond that stay idle.

## Ordering

Kafka orders records per topic-partition, and the connector preserves that
ordering end to end. Each partition is pulled by exactly one producer, which
emits it in offset order, and the connector sets Broadway's `:partition_by` so
that all records of a given `{topic, partition}` are always handled by the same
processor (and batcher) stage. Different partitions are still processed
concurrently.

The connector owns `:partition_by`; setting it yourself raises. Scale out
across partitions with processor/batcher concurrency.

## Development

```sh
mix deps.get
mix test       # unit tests for the offset-tracking core (no broker needed)
```
