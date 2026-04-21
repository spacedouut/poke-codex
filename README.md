# poke-codex

A FastMCP server that uses Codex CLI and the `poke` Python SDK so Poke can orchestrate local Codex sessions with streaming updates.

## What changed in this version

This server now bootstraps Poke integration automatically on startup:

1. Starts a Poke tunnel process immediately.
2. Sends a Poke message that Codex is connected.
3. Exposes only Codex-oriented MCP tools (no Poke self-messaging/tunnel management tools).

## Why this design

- Poke orchestration only needs a live MCP endpoint and tunnel to reach local Codex.
- Codex execution remains process-driven through `codex exec --json` for practical session control.
- `poke.mcp.with_callbacks` + `PokeCallbackMiddleware` enable progressive response streaming back to Poke.

## Available MCP tools

- `run_codex_stream`: run `codex exec --json` and stream output line-by-line.
- `interrupt_codex_session`: stop an active Codex run.
- `list_codex_sessions`: inspect tracked sessions and recent output.

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Node.js + `npx` (for `npx poke@latest tunnel ...`)
- Codex CLI installed and authenticated (`codex` on PATH)
- Poke credentials (`POKE_API_KEY` or local credentials from `poke login`)

## Setup

```bash
uv sync
uv run poke-codex-mcp
```

Defaults:
- host: `0.0.0.0`
- port: `3000`
- path: `/mcp`

## Startup behavior

On server startup:
- The server launches `npx poke@latest tunnel <local-mcp-url> -n <name>`.
- The server sends a Poke message saying Codex is connected.

On shutdown:
- The tunnel process is stopped with `SIGINT`.

## Environment variables

- `HOST` (default: `0.0.0.0`)
- `PORT` (default: `3000`)
- `MCP_PATH` (default: `/mcp`)
- `POKE_MCP_LOCAL_URL` (override auto-derived local URL)
- `POKE_TUNNEL_NAME` (default: `Local Codex MCP`)
- `POKE_CONNECTED_MESSAGE` (custom startup notification text)
- `CODEX_CMD` (default: `codex`)
