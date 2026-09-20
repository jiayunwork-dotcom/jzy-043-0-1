#!/usr/bin/env bash
# Seed TaskForge with a mix of traffic to immediately visualize the system.
set -u
API="${API:-http://localhost:8080}"

post() {
  curl -sS -X POST "$API/api/tasks" -H 'Content-Type: application/json' -d "$1" >/dev/null
  echo "submitted: $2"
}

# Critical / High / Normal / Low / Bulk successes
post '{"type":"compute","payload":{"a":1,"b":2,"op":"add"},"priority":"critical"}' "critical compute"
post '{"type":"compute","payload":{"a":3,"b":4,"op":"mul"},"priority":"high"}' "high compute"
post '{"type":"sleep","payload":{"sleep_ms":120},"priority":"normal"}' "normal sleep"
post '{"type":"echo","payload":{"msg":"low"},"priority":"low"}' "low echo"
post '{"type":"echo","payload":{"msg":"bulk"},"priority":"bulk"}' "bulk echo"

# Delayed tasks (5s / 15s)
post '{"type":"echo","payload":{"delayed":5},"priority":"normal","delay_seconds":5}' "delayed 5s"
post '{"type":"echo","payload":{"delayed":15},"priority":"low","delay_seconds":15}' "delayed 15s"

# Flaky task that fails twice then succeeds (fixed retry)
post '{"type":"flaky","payload":{"fail_first_n":2},"priority":"high","max_retries":3,
      "retry_policy":{"kind":"fixed","base_interval_seconds":1,"max_retries":3}}' "flaky retries-then-ok"

# Exponential backoff demo
post '{"type":"flaky","payload":{"fail_first_n":1},"priority":"normal","max_retries":2,
      "retry_policy":{"kind":"exponential","base_interval_seconds":1,"max_retries":2}}' "exponential backoff"

# Tasks that exhaust retries -> dead letter (two different error types)
for i in 1 2 3; do
  post '{"type":"always-fail","payload":{},"priority":"bulk","max_retries":2,
        "retry_policy":{"kind":"fixed","base_interval_seconds":1,"max_retries":2}}' "always-fail #$i"
done

echo
echo "Seed complete. Open the panel at http://localhost:3000"
