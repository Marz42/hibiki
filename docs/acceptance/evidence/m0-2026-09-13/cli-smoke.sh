set -x
export UV_CACHE_DIR=/tmp/hibiki-uv-cache
H="uv run --no-sync hibiki --data-dir /tmp/m0-fix/db/cli --json"
rm -rf /tmp/m0-fix/db/cli
echo "### 1. empty data dir -> migrate on first command"
$H get-task --task-id missing; echo "EXIT=$?"
echo "### 2. create-task"
CREATE=$($H create-task --title "M0 CLI smoke" 2>/dev/null)
echo "$CREATE"
TASK_ID=$(echo "$CREATE" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['task_id'])")
echo "### 3. submit-contract"
SUB=$($H submit-contract --task-id "$TASK_ID" 2>/dev/null)
echo "$SUB"
DEC=$(echo "$SUB" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['decision_id'])")
CH=$(echo "$SUB" | python3 -c "import json,sys; print(json.load(sys.stdin)['data']['content_hash'])")
echo "### 4. H-003: approval against a stale target hash is refused"
$H approve-contract --decision-id "$DEC" --expected-hash deadbeef; echo "EXIT=$?"
echo "### 5. H-002: USER_AGENT self-reporting as approver is refused"
uv run --no-sync hibiki --data-dir /tmp/m0-fix/db/cli --json --principal ua_1 --actor ua_1 --actor-type USER_AGENT approve-contract --decision-id "$DEC" --expected-hash "$CH"; echo "EXIT=$?"
echo "### 6. HUMAN approval against the frozen content hash"
$H approve-contract --decision-id "$DEC" --expected-hash "$CH"; echo "EXIT=$?"
echo "### 7. H-004: duplicate approval replays, no second formal decision"
$H approve-contract --decision-id "$DEC" --expected-hash "$CH"; echo "EXIT=$?"
echo "### 8. activate-minimal-plan + dispatch"
$H activate-minimal-plan --task-id "$TASK_ID" 2>/dev/null
$H dispatch --task-id "$TASK_ID" 2>/dev/null
echo "### 9. list-runs (agent started by the Fake adapter)"
$H list-runs --task-id "$TASK_ID" 2>/dev/null
echo "### 10. H-017: HUMAN cannot submit a worker result (Internal-only)"
$H submit-result --run-id "$(uv run --no-sync python -c "
import sqlite3; c=sqlite3.connect('/tmp/m0-fix/db/cli/hibiki.db'); print(c.execute('select run_id from agent_runs').fetchone()[0])")" --outcome COMPLETED --verdict PASS; echo "EXIT=$?"
echo "### 11. task state after the refused submission"
$H get-task --task-id "$TASK_ID" 2>/dev/null
