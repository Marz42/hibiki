# M1 harness smoke evidence (fake provider, not the acceptance run)

This is **not** the M1 acceptance record. It proves that the whole M1 path works end to
end before real credentials exist:

- provider: `tools/fake_openai_server.py`, a local OpenAI-compatible HTTP double
  (the adapter still speaks real HTTP to `/chat/completions`);
- Core: real `ApplicationService` with the real `ToolBroker`;
- sandbox: the real hardened Docker container (`hibiki-sandbox:py312`), running pytest
  for the k3 task.

Command:

```bash
HIBIKI_MODEL_BASE_URL=http://127.0.0.1:8769/v1 HIBIKI_MODEL_API_KEY=sk-fake \
HIBIKI_MODEL=fake-model HIBIKI_SANDBOX_IMAGE=hibiki-sandbox:py312 \
uv run --no-sync python -m hibiki.interfaces.m1_runner \
  --data-dir /tmp/m1-smoke --out /tmp/m1-smoke-out --repeats 2
```

Result: 6/6 runs submitted a COMPLETED/PASS result with verified artifacts
(k1 summary.txt, k2 output.json, k3 sample.py after a real in-sandbox pytest run).
Every recorded run also carries its frozen spec hash, context manifest id, granted
tools, per-tool decisions/outcomes and artifact verification.

The real §24.4 gate requires these same three tasks against a real model with the
operator's credentials; that record lives in `docs/acceptance/M1-<date>.md`.
