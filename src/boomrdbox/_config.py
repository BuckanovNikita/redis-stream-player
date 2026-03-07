"""Hydra-zen programmatic config store."""

from __future__ import annotations

from hydra_zen import ZenStore, make_config

from boomrdbox.models import (
    ConvertConf,
    InfoConf,
    PlayConf,
    RecordConf,
    RedisConf,
    StreamItemConf,
    StreamsConf,
    TruncateConf,
)

store = ZenStore()

# ---------------------------------------------------------------------------
# Redis config group
# ---------------------------------------------------------------------------
store(
    RedisConf(host="localhost", port=6379, db=0, password=None),
    name="local",
    group="redis",
)
store(
    RedisConf(host="prod-redis", port=6379, db=0, password=None),
    name="prod",
    group="redis",
)

# ---------------------------------------------------------------------------
# Streams config group
# ---------------------------------------------------------------------------
store(
    StreamsConf(
        streams=[
            StreamItemConf(
                key="sensor:imu",
                timestamp_field="receive_ts",
                timestamp_mode="bypass",
            ),
            StreamItemConf(key="sensor:gps"),
            StreamItemConf(
                key="sensor:camera",
                timestamp_field="ts_nano",
                timestamp_mode="shift",
            ),
        ],
    ),
    name="sensors",
    group="streams",
)
store(
    StreamsConf(
        streams=[
            StreamItemConf(key="events:user"),
            StreamItemConf(key="events:system"),
            StreamItemConf(key="events:audit"),
        ],
    ),
    name="events",
    group="streams",
)

# ---------------------------------------------------------------------------
# Top-level configs
# ---------------------------------------------------------------------------
RecordConfig = make_config(
    hydra_defaults=[
        "_self_",
        {"redis": "local"},
        {"streams": "sensors"},
    ],
    bases=(RecordConf,),
)
store(RecordConfig, name="record")

PlayConfig = make_config(
    hydra_defaults=[
        "_self_",
        {"redis": "local"},
        {"streams": "sensors"},
    ],
    bases=(PlayConf,),
)
store(PlayConfig, name="play")

store(ConvertConf, name="convert")
store(TruncateConf, name="truncate")
store(InfoConf, name="info")
