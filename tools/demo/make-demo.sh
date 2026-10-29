#!/usr/bin/env bash
# Records demokcg.gif, the README's animated demo: the real binary, driven by
# VHS (https://github.com/charmbracelet/vhs) in a real terminal, against the
# Kafka 4.4 of the kafkabeat Docker bench (container kafka44, localhost:19094).
#
#   tools/demo/make-demo.sh [binary [gif]]   (defaults: bin/kcg, demokcg.gif)
#
# Linux only (WSL included). Needs vhs, ttyd, ffmpeg and docker; VHS downloads
# the headless Chromium it renders with on its first run. The topic and the
# groups are deleted and created again on every run, so every recording starts
# from the same offsets.
set -eu

repo=$(cd "$(dirname "$0")/../.." && pwd)
binary=$(realpath -m "${1:-$repo/bin/kcg}")
output=$(realpath -m "${2:-$repo/demokcg.gif}")
bootstrap=localhost:19094

for tool in vhs ttyd ffmpeg docker; do
  command -v "$tool" >/dev/null || { echo "$tool not found" >&2; exit 1; }
done
[ -x "$binary" ] || { echo "no kcg binary at $binary (make build)" >&2; exit 1; }

kafka() { # kafka TOOL ARGS...: one Kafka CLI tool inside the bench container
  local tool=$1; shift
  docker exec -e KAFKA_OPTS= kafka44 "/opt/kafka/bin/$tool.sh" --bootstrap-server localhost:9092 "$@"
}
groups=(billing billing-v2 billing-old)
cleanup() {
  kafka kafka-consumer-groups $(printf -- '--group %s ' "${groups[@]}") --delete >/dev/null 2>&1 || true
  kafka kafka-topics --delete --topic orders >/dev/null 2>&1 || true
}

# --- Data: topic orders (3 partitions, about 10,000 messages each), three groups
cleanup
until ! kafka kafka-topics --list | grep -qx orders; do sleep 1; done
kafka kafka-topics --create --topic orders --partitions 3 >/dev/null
docker exec -e KAFKA_OPTS= kafka44 /opt/kafka/bin/kafka-producer-perf-test.sh --topic orders \
  --num-records 30000 --record-size 10 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 >/dev/null
work=$(mktemp -d)
trap 'cleanup; rm -rf "$work"' EXIT
mkdir -p "$work/bin" && cp "$binary" "$work/bin/kcg"
cd "$work"
kcg() { "$work/bin/kcg" "$1" --bootstrap-server "$bootstrap" "${@:2}" >/dev/null; }
kcg align --topic orders --from 1200 --to billing --execute
kcg align --topic orders --from 2600 --to billing-v2 --execute
kcg align --topic orders --from 400 --to billing-old --execute
rm -f kcg-backup-*.csv

b="--bootstrap-server $bootstrap"
cat > "$work/demo.tape" <<EOF
Output "$output"
Set Shell bash
Set FontFamily "DejaVu Sans Mono"
Set FontSize 20
Set Width 1600
Set Height 640
Set Padding 12
Set TypingSpeed 35ms
Set WaitTimeout 20s
Set Framerate 25

Hide
Type "export PATH=$work/bin:\$PATH PS1='$ '; cd $work; clear"
Enter
Show

Sleep 600ms
Type "kcg read $b --topic orders --group 'billing*'"
Enter
Wait+Screen /most advanced/
Sleep 3500ms

Type "kcg align $b --topic orders --from billing-v2 --to billing"
Enter
Wait+Screen /Dry run/
Sleep 3500ms
Ctrl+L

Type "kcg align $b --topic orders --from billing-v2 --to billing --execute"
Enter
Wait+Screen /Applied/
Sleep 3500ms
Ctrl+L

Type "kcg clean $b --topic orders --group 'billing*'"
Enter
Wait+Screen /identical offsets/
Sleep 4000ms

Type "kcg clean $b --topic orders --group 'billing*' --keep billing-v2 --execute"
Enter
Wait+Screen /Applied/
Sleep 3500ms
Ctrl+L

Type "kcg read $b --topic orders --group 'billing*'"
Enter
Wait+Screen /most advanced/
Sleep 4000ms
EOF
vhs "$work/demo.tape"
ls -l "$output"
