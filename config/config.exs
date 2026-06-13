import Config

import_config "#{config_env()}.exs"

config :off_broadway_klife, OffBroadwayKlife.TestClient,
  connection: [
    bootstrap_servers: ["localhost:19092", "localhost:29092"],
    ssl: false
  ],
  enable_unkown_topics: true

config :klife, metadata_check_interval_ms: 1000
