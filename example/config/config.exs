import Config

config :off_broadway_klife_example, OffBroadwayKlifeExample.KafkaClient,
  connection: [
    bootstrap_servers: ["localhost:19092", "localhost:29092"],
    ssl: false
  ]

config :klife, metadata_check_interval_ms: 1000
