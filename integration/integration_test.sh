#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Integration test for boomrdbox CLI
# ---------------------------------------------------------------------------

REDIS_HOST=localhost
REDIS_PORT=6389
REDIS_DB_RECORD=0
REDIS_DB_PLAY=1
KEEP_REDIS=false
WORKDIR=$(mktemp -d)
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)
PASS_COUNT=0
FAIL_COUNT=0
STEP_TOTAL=7
REDIS_CONTAINER=redis-stream-player-redis-1

for arg in "$@"; do
    case $arg in
        --keep-redis) KEEP_REDIS=true ;;
    esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

step() {
    local num=$1; shift
    echo -e "\n${BLUE}=== Step ${num}/${STEP_TOTAL}: $*${NC}"
}

pass() {
    echo -e "  ${GREEN}PASS${NC}: $*"
    PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
    echo -e "  ${RED}FAIL${NC}: $*"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

rcli() {
    docker exec $REDIS_CONTAINER redis-cli "$@"
}

assert_file_not_empty() {
    local f=$1
    if [ -s "$f" ]; then
        pass "$f exists and is non-empty"
    else
        fail "$f missing or empty"
    fi
}

assert_gt_zero() {
    local val=$1 label=$2
    if [ "$val" -gt 0 ] 2>/dev/null; then
        pass "$label = $val > 0"
    else
        fail "$label = $val, expected > 0"
    fi
}

within_tolerance() {
    local actual=$1 expected=$2 pct=$3 label=$4
    if [ "$expected" -eq 0 ]; then
        fail "$label: expected is 0"
        return
    fi
    local diff=$((actual - expected))
    if [ $diff -lt 0 ]; then diff=$((-diff)); fi
    local threshold=$(( expected * pct / 100 ))
    if [ $diff -le $threshold ]; then
        pass "$label: $actual within ${pct}% of $expected"
    else
        fail "$label: $actual NOT within ${pct}% of $expected (diff=$diff, threshold=$threshold)"
    fi
}

cleanup() {
    echo -e "\n${BLUE}=== Cleanup${NC}"
    rm -rf "$WORKDIR"
    echo "  Removed $WORKDIR"
    if [ "$KEEP_REDIS" = false ]; then
        cd "$PROJECT_ROOT" && docker compose down --timeout 5 2>/dev/null || true
        echo "  Stopped Redis"
    else
        echo "  Keeping Redis running (--keep-redis)"
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: Start Redis
# ---------------------------------------------------------------------------
step 1 "Start Redis"
cd "$PROJECT_ROOT"
docker compose up -d redis

echo "  Waiting for Redis..."
for i in $(seq 1 30); do
    if rcli ping 2>/dev/null | grep -q PONG; then
        pass "Redis is ready (attempt $i)"
        break
    fi
    if [ "$i" -eq 30 ]; then
        fail "Redis did not become ready in 30s"
        exit 1
    fi
    sleep 1
done

rcli -n $REDIS_DB_RECORD FLUSHDB > /dev/null
rcli -n $REDIS_DB_PLAY FLUSHDB > /dev/null
echo "  Flushed db=$REDIS_DB_RECORD and db=$REDIS_DB_PLAY"

# ---------------------------------------------------------------------------
# Step 2: Build & install wheel
# ---------------------------------------------------------------------------
step 2 "Build & install wheel"
cd "$PROJECT_ROOT"
uv build 2>&1 | tail -1
WHEEL=$(ls -t dist/boomrdbox-*.whl 2>/dev/null | head -1)
if [ -z "$WHEEL" ]; then
    fail "No wheel found in dist/"
    exit 1
fi
uv pip install "$WHEEL" --force-reinstall 2>&1 | tail -1

if boomrdbox > /dev/null 2>&1; then
    pass "boomrdbox CLI is available"
else
    fail "boomrdbox CLI not found"
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 3: Fill Redis with test data
# ---------------------------------------------------------------------------
step 3 "Fill Redis with test data"
FILL_OUTPUT=$(uv run python integration/fill_redis.py \
    --host $REDIS_HOST --port $REDIS_PORT \
    --db $REDIS_DB_RECORD --duration-sec 120)

echo "$FILL_OUTPUT"

BASE_TIMESTAMP_MS=$(echo "$FILL_OUTPUT" | grep BASE_TIMESTAMP_MS | awk '{print $2}')
TOTAL_MESSAGES=$(echo "$FILL_OUTPUT" | grep TOTAL_MESSAGES | awk '{print $2}')

assert_gt_zero "$TOTAL_MESSAGES" "TOTAL_MESSAGES"

IMU_LEN=$(rcli -n $REDIS_DB_RECORD XLEN sensor:imu)
within_tolerance "$IMU_LEN" 12000 5 "sensor:imu XLEN"

# ---------------------------------------------------------------------------
# Step 4: Record (CLI param overrides)
# ---------------------------------------------------------------------------
step 4 "Record (CLI overrides)"

boomrdbox record \
    redis.host=$REDIS_HOST redis.port=$REDIS_PORT redis.db=$REDIS_DB_RECORD \
    from_beginning=true \
    output=$WORKDIR/recording.msgpack \
    max_duration=10 \
    hydra.run.dir=$WORKDIR

assert_file_not_empty "$WORKDIR/recording.msgpack"

echo "  --- info ---"
boomrdbox info input=$WORKDIR/recording.msgpack hydra.run.dir=$WORKDIR
echo "  --- end info ---"

pass "Record completed"

# ---------------------------------------------------------------------------
# Step 5: Truncate (YAML config template)
# ---------------------------------------------------------------------------
step 5 "Truncate (YAML config)"

FROM_ID=${BASE_TIMESTAMP_MS}-0
TO_ID=$((BASE_TIMESTAMP_MS + 60000))-0

cp "$PROJECT_ROOT/integration/truncate_cfg.yaml" "$WORKDIR/truncate_cfg.yaml"
sed -i "s|PLACEHOLDER_INPUT|$WORKDIR/recording.msgpack|" "$WORKDIR/truncate_cfg.yaml"
sed -i "s|PLACEHOLDER_OUTPUT|$WORKDIR/truncated.msgpack|" "$WORKDIR/truncate_cfg.yaml"
sed -i "s|PLACEHOLDER_FROM_ID|$FROM_ID|" "$WORKDIR/truncate_cfg.yaml"
sed -i "s|PLACEHOLDER_TO_ID|$TO_ID|" "$WORKDIR/truncate_cfg.yaml"

echo "  Generated truncate config:"
cat "$WORKDIR/truncate_cfg.yaml"

T_INPUT=$(grep input "$WORKDIR/truncate_cfg.yaml" | awk '{print $2}')
T_OUTPUT=$(grep output "$WORKDIR/truncate_cfg.yaml" | awk '{print $2}')
T_FROM_ID=$(grep from_id "$WORKDIR/truncate_cfg.yaml" | awk '{print $2}')
T_TO_ID=$(grep to_id "$WORKDIR/truncate_cfg.yaml" | awk '{print $2}')

boomrdbox truncate \
    input=$T_INPUT output=$T_OUTPUT \
    from_id=$T_FROM_ID to_id=$T_TO_ID \
    hydra.run.dir=$WORKDIR

assert_file_not_empty "$WORKDIR/truncated.msgpack"

echo "  --- truncated info ---"
boomrdbox info input=$WORKDIR/truncated.msgpack hydra.run.dir=$WORKDIR
echo "  --- end truncated info ---"

pass "Truncate completed"

# ---------------------------------------------------------------------------
# Step 6: Play (env vars)
# ---------------------------------------------------------------------------
step 6 "Play (env vars for Redis DB)"

REDIS_HOST=$REDIS_HOST REDIS_PORT=$REDIS_PORT REDIS_DB=$REDIS_DB_PLAY \
    boomrdbox play \
        input=$WORKDIR/truncated.msgpack \
        speed=1000.0 \
        hydra.run.dir=$WORKDIR

pass "Play completed"

# ---------------------------------------------------------------------------
# Step 7: Verify
# ---------------------------------------------------------------------------
step 7 "Verify playback results"

PLAY_IMU=$(rcli -n $REDIS_DB_PLAY XLEN sensor:imu)
PLAY_GPS=$(rcli -n $REDIS_DB_PLAY XLEN sensor:gps)
PLAY_CAM=$(rcli -n $REDIS_DB_PLAY XLEN sensor:camera)

assert_gt_zero "$PLAY_IMU" "play db sensor:imu XLEN"
assert_gt_zero "$PLAY_GPS" "play db sensor:gps XLEN"
assert_gt_zero "$PLAY_CAM" "play db sensor:camera XLEN"

# Truncated window is 60s out of 120s, so expect roughly half of fill counts
within_tolerance "$PLAY_IMU" 6000 10 "play sensor:imu vs expected ~6000"
within_tolerance "$PLAY_GPS" 60 10 "play sensor:gps vs expected ~60"
within_tolerance "$PLAY_CAM" 1818 10 "play sensor:camera vs expected ~1818"

# Spot-check fields
IMU_SAMPLE=$(rcli -n $REDIS_DB_PLAY XRANGE sensor:imu - + COUNT 1)
for field in x y z receive_ts; do
    if echo "$IMU_SAMPLE" | grep -q "$field"; then
        pass "sensor:imu has field $field"
    else
        fail "sensor:imu missing field $field"
    fi
done

GPS_SAMPLE=$(rcli -n $REDIS_DB_PLAY XRANGE sensor:gps - + COUNT 1)
for field in lat lon; do
    if echo "$GPS_SAMPLE" | grep -q "$field"; then
        pass "sensor:gps has field $field"
    else
        fail "sensor:gps missing field $field"
    fi
done

CAM_SAMPLE=$(rcli -n $REDIS_DB_PLAY XRANGE sensor:camera - + COUNT 1)
for field in frame ts_nano width height; do
    if echo "$CAM_SAMPLE" | grep -q "$field"; then
        pass "sensor:camera has field $field"
    else
        fail "sensor:camera missing field $field"
    fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo -e "${BLUE}=== Summary${NC}"
echo -e "  ${GREEN}PASS: $PASS_COUNT${NC}"
if [ $FAIL_COUNT -gt 0 ]; then
    echo -e "  ${RED}FAIL: $FAIL_COUNT${NC}"
    exit 1
else
    echo -e "  ${RED}FAIL: 0${NC}"
    echo -e "  ${GREEN}All checks passed!${NC}"
    exit 0
fi
