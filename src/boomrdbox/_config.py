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
_LocalRedisConf = make_config(
    host="${oc.env:REDIS_HOST,localhost}",
    port="${oc.env:REDIS_PORT,6379}",
    db="${oc.env:REDIS_DB,0}",
    password="${oc.decode:${oc.env:REDIS_PASSWORD,null}}",  # noqa: S106
    bases=(RedisConf,),
)
store(_LocalRedisConf, name="local", group="redis")

_ProdRedisConf = make_config(
    host="${oc.env:REDIS_HOST,prod-redis}",
    port="${oc.env:REDIS_PORT,6379}",
    db="${oc.env:REDIS_DB,0}",
    password="${oc.decode:${oc.env:REDIS_PASSWORD,null}}",  # noqa: S106
    bases=(RedisConf,),
)
store(_ProdRedisConf, name="prod", group="redis")

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
