import Config

config :broadway_klife_example, BroadwayKlifeExample.KafkaClient,
  connection: [
    bootstrap_servers: ["localhost:19092", "localhost:29092"],
    ssl: false
  ]

config :klife, metadata_check_interval_ms: 1000
